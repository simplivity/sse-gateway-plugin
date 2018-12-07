/*
 * The MIT License
 *
 * Copyright (c) 2016, CloudBees, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package org.jenkinsci.plugins.ssegateway.sse;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nonnull;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSessionEvent;

import org.acegisecurity.Authentication;
import org.jenkinsci.plugins.pubsub.ChannelSubscriber;
import org.jenkinsci.plugins.pubsub.EventFilter;
import org.jenkinsci.plugins.pubsub.EventProps;
import org.jenkinsci.plugins.pubsub.Message;
import org.jenkinsci.plugins.pubsub.MessageException;
import org.jenkinsci.plugins.pubsub.PubsubBus;
import org.jenkinsci.plugins.pubsub.SimpleMessage;
import org.jenkinsci.plugins.ssegateway.EventHistoryStore;
import org.jenkinsci.plugins.ssegateway.Util;
import org.kohsuke.accmod.Restricted;
import org.kohsuke.accmod.restrictions.NoExternalUse;

import hudson.Extension;
import hudson.model.User;
import hudson.util.CopyOnWriteMap;
import jenkins.model.Jenkins;
import jenkins.util.HttpSessionListener;

import net.sf.json.JSONObject;

/**
 * @author <a href="mailto:tom.fennelly@gmail.com">tom.fennelly@gmail.com</a>
 */
@Restricted(NoExternalUse.class)
public abstract class EventDispatcher implements Serializable {

    public static final String SESSION_SYNC_OBJ = "org.jenkinsci.plugins.ssegateway.sse.session.sync";
    private static final Logger LOGGER = Logger.getLogger(EventDispatcher.class.getName());

    private String id = null;
    private final transient PubsubBus bus;
    private final transient Authentication authentication;
    private transient Map<EventFilter, ChannelSubscriber> subscribers = new CopyOnWriteMap.Hash<>();

    private static final int MAX_RETRY_COUNT = 20000; //TODO: reduce back
    private static final int MAX_RELOAD_COUNT = 20000; //TODO: reduce back

    private int reloadCount = 0;
    private long creationDate = System.currentTimeMillis();
    private volatile long lastRetryClear = 0;
    private volatile long lastHandledRetryTime = 0;
    private volatile long lastCreatedRetryTime = 0;

    public EventDispatcher() {
        this.bus = PubsubBus.getBus();
        User current = getUser();
        if (current != null) {
            this.authentication = Jenkins.getAuthentication();
        } else {
            this.authentication = Jenkins.ANONYMOUS;
        }
    }

    public abstract void start(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException;
    public abstract HttpServletResponse getResponse();

    public Map<EventFilter, ChannelSubscriber> getSubscribers() {
        return Collections.unmodifiableMap(subscribers);
    }

    public final String getId() {
        if (id == null) {
            throw new IllegalStateException("Call to getId before the ID ewas set.");
        }
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return String.format("%s (%s)", id, System.identityHashCode(this));
    }

    /**
     * Writes a message to {@link HttpServletResponse}
     *
     * @return
     *      false if the response is not writable
     */
    public synchronized boolean dispatchEvent(String name, String data) throws IOException, ServletException {
        HttpServletResponse response = getResponse();

        if (response == null) {
            // The SSE channel is not connected or is reconnecting after timeout.
            // Event will go to retry queue.
            return false;
        }

        if (LOGGER.isLoggable(Level.FINEST)) {
            LOGGER.log(Level.FINEST, String.format("SSE dispatcher %s sending event: %s", this, data));
        }

        PrintWriter writer = response.getWriter();

        if (writer.checkError()) {
            return false;
        }

        if (name != null) {
            writer.write("event: " + name + "\n");
        }
        if (data != null) {
            writer.write("data: " + data + "\n");
        }
        writer.write("\n");

        return (!writer.checkError());
    }

    public void stop() {
        // override as needed
    }

    void setDefaultHeaders() {
        HttpServletResponse response = getResponse();
        response.setStatus(200);
        response.setContentType("text/event-stream");
        response.setCharacterEncoding("UTF-8");
        response.setHeader("Cache-Control","no-cache");
        response.setHeader("Connection","keep-alive");
    }

    public boolean subscribe(@Nonnull EventFilter filter) {
        String channelName = filter.getChannelName();

        if (channelName != null) {
            SSEChannelSubscriber subscriber = (SSEChannelSubscriber) subscribers.get(filter);
            if (subscriber == null) {
                subscriber = new SSEChannelSubscriber();

                bus.subscribe(channelName, subscriber, authentication, filter);
                subscribers.put(filter, subscriber);
            } else {
                // Already subscribed to this event.
            }

            subscriber.numSubscribers++;
            publishStateEvent(SSEChannel.Event.subscribe,  new SimpleMessage()
                    .set(SSEChannel.EventProps.sse_subs_dispatcher, id)
                    .set(SSEChannel.EventProps.sse_subs_channel_name, channelName)
                    .set(SSEChannel.EventProps.sse_subs_filter, filter.toJSON())
            );

            return true;
        } else {
            LOGGER.log(Level.SEVERE, String.format("Invalid SSE subscribe configuration. '%s' not specified.", EventProps.Jenkins.jenkins_channel));
        }

        return false;
    }

    protected User getUser() {
        return User.current();
    }

    public boolean unsubscribe(@Nonnull EventFilter filter) {
        String channelName = filter.getChannelName();
        if (channelName != null) {
            SSEChannelSubscriber subscriber = (SSEChannelSubscriber) subscribers.get(filter);
            if (subscriber != null) {
                subscriber.numSubscribers--;
                if (subscriber.numSubscribers == 0) {
                    try {
                        bus.unsubscribe(channelName, subscriber);
                    } finally {
                        subscribers.remove(filter);
                    }
                }
                publishStateEvent(SSEChannel.Event.unsubscribe,  new SimpleMessage()
                        .set(SSEChannel.EventProps.sse_subs_dispatcher, id)
                        .set(SSEChannel.EventProps.sse_subs_channel_name, channelName)
                        .set(SSEChannel.EventProps.sse_subs_filter, filter.toJSON())
                );
                return true;
            } else {
                LOGGER.log(Level.FINE, "Invalid SSE unsubscribe configuration. No active subscription for channel: " + channelName);
            }
        } else {
            LOGGER.log(Level.SEVERE, String.format("Invalid SSE unsubscribe configuration. '%s' not specified.", EventProps.Jenkins.jenkins_channel));
        }
        return false;
    }

    public void unsubscribeAll() {
        Set<Map.Entry<EventFilter, ChannelSubscriber>> entries = subscribers.entrySet();
        for (Map.Entry<EventFilter, ChannelSubscriber> entry : entries) {
            SSEChannelSubscriber subscriber = (SSEChannelSubscriber) entry.getValue();
            EventFilter filter = entry.getKey();
            String channelName = filter.getChannelName();

            bus.unsubscribe(channelName, subscriber);
        }
        subscribers.clear();
    }

    private void publishStateEvent(SSEChannel.Event event, Message additional) {
        // Only publish these events if we're running
        // in a test.
        if (!Util.isTestEnv()) {
            return;
        }

        try {
            SimpleMessage message = new SimpleMessage()
                    .setChannelName("sse")
                    .setEventName(event)
                    .set("sse_numsubs", Integer.toString(subscribers.size()));
            if (additional != null) {
                message.putAll(additional);
            }
            bus.publish(message);
        } catch (MessageException e) {
            LOGGER.log(Level.WARNING, "Failed to publish SSE Dispatcher state event.", e);
        }
    }

    private void dispatchReload() {
        lastRetryClear = System.currentTimeMillis();
        try {
            reloadCount++;
            if (reloadCount < MAX_RELOAD_COUNT) {
                dispatchEvent("reload", null);
            } else {
                LOGGER.log(Level.SEVERE, "Reached max reload count, exhausted, created at: " + (System.currentTimeMillis() - creationDate) + "ms ago");
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Unable to send reload event to client.", e);
        }
    }

    private void addToRetryQueue(@Nonnull Message message) { addToRetryQueue(new Retry(message, this)); }

    private void addToRetryQueue(Retry retry) {
        this.lastCreatedRetryTime = Math.max(lastCreatedRetryTime, retry.updateLastUpdateTime());
        RetryManager.getInstance().add(retry);
    }

    public void processRetry(Retry retry) {
        this.lastHandledRetryTime = Math.max(lastHandledRetryTime, retry.getLastUpdateTime());
        if (retry.getLastUpdateTime() < lastRetryClear || retry.isExpired()){
            return;
        }

        try {
            String eventJSON = EventHistoryStore.getChannelEvent(retry.getChannelName(), retry.getEventUUID());

            if (eventJSON == null) {
                // The event is not in the store. This can be simply because the event has
                // not yet arrived at the store and been stored. It might need another
                // moment or two to get there.
                if (!retry.needsMoreTimeToLandInStore()) {
                    // Something's gone wrong. The event should be in the store by now.
                    // Lets tell the client that it needs to do a full page reload. Not much
                    // else can be done at this stage.
                    dispatchReload(); // This clears the queue too.
                    return;
                } else {
                    // The event should be in the store (not expired, so would not
                    // have been deleted). Only explanation is that it has not yet
                    // landed there. Let's pause the retry process
                    // for a moment and retry again in the hope that the event
                    // eventually lands there. Exiting here will schedule a new run
                    // of this retry process (see finally block below).
                    if (retry.incRetryCount() > MAX_RETRY_COUNT){
                        return;
                    }
                }
            }

            if (Util.isTestEnv()) {
                JSONObject eventJSONObj = JSONObject.fromObject(eventJSON);
                eventJSONObj.put(SSEChannel.EventProps.sse_dispatch_retry.name(), "true");
                eventJSON = eventJSONObj.toString();
            }

            if (!dispatchEvent(retry.getChannelName(), eventJSON)) {
                LOGGER.log(Level.FINE, String.format("Error dispatching retry event to SSE channel. Write failed. Dispatcher %s.", this));
                if (retry.incRetryCount() > MAX_RETRY_COUNT){
                    return;
                }
            } else if (LOGGER.isLoggable(Level.FINEST)) {
                LOGGER.log(Level.FINEST, "Dispatched retry event to SSE channel. Dispatcher {0}. Event {1}.", new Object[] {this, eventJSON});
            }
        } catch (Exception e) {
            LOGGER.log(Level.FINE, String.format("Error dispatching retry event to SSE channel. Write failed. Dispatcher %s.", this), e);
            if (retry.incRetryCount() > MAX_RETRY_COUNT){
                return;
            }
        }

        addToRetryQueue(retry);
    }

    public long getCreationDate() { return creationDate; }

    private void doDispatch(@Nonnull Message message) {
        if (lastCreatedRetryTime > lastHandledRetryTime) {
            // We do not attempt to dispatch events directly
            // while there are events sitting in the retryQueue.
            // The retryQueue must be empty.
            addToRetryQueue(message);
        } else {
            try {
                message.set(SSEChannel.EventProps.sse_subs_dispatcher, this.id);
                message.set(SSEChannel.EventProps.sse_subs_dispatcher_inst, Integer.toString(System.identityHashCode(this)));

                if (!dispatchEvent(message.getChannelName(), message.toJSON())) {
                    LOGGER.log(Level.FINE, "Error dispatching event to SSE channel. Write failed.");
                    addToRetryQueue(message);
                }
            } catch (Exception e) {
                LOGGER.log(Level.FINE, "Error dispatching event to SSE channel.", e);
                addToRetryQueue(message);
            }
        }
    }

    /**
     * Receive event from {@link PubsubBus} and sends it to this client.
     */
    private final class SSEChannelSubscriber implements ChannelSubscriber {
        private int numSubscribers = 0;

        @Override
        public void onMessage(@Nonnull Message message) {
            doDispatch(message);
        }
    }

    /**
     * Http session listener.
     */
    @Extension
    public static final class SSEHttpSessionListener extends HttpSessionListener {
        @Override
        public void sessionDestroyed(HttpSessionEvent httpSessionEvent) {
            try {
                Map<String, EventDispatcher> dispatchers = EventDispatcherFactory.getDispatchers(httpSessionEvent.getSession());
                try {
                    for (EventDispatcher dispatcher : dispatchers.values()) {
                        try {
                            dispatcher.unsubscribeAll();
                        } catch (Exception e) {
                            LOGGER.log(Level.FINE, "Error during unsubscribeAll() for dispatcher " + dispatcher.getId() + ".", e);
                        }
                    }
                } finally {
                    dispatchers.clear();
                }
            } catch (Exception e) {
                LOGGER.log(Level.FINE, "Error during session cleanup. The session has probably timed out.", e);
            }
        }
    }


}
