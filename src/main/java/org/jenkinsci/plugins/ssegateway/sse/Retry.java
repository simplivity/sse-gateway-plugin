package org.jenkinsci.plugins.ssegateway.sse;

import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import org.jenkinsci.plugins.pubsub.Message;

public class Retry {
    private static final long EXPIRY_TIME_MS = TimeUnit.MINUTES.toMillis(30); //TODO: CHANGE back to 3
    private final long timestamp = System.currentTimeMillis();
    private final String channelName;
    private final String eventUUID;
    private int retryCount = 0;
    private EventDispatcher eventDispatcher;
    private long lastUpdateTime = timestamp;

    public Retry(@Nonnull Message message, EventDispatcher eventDispatcher) {
        // We want to keep the memory footprint of the retryQueue
        // to a minimum. That is why we are interning these strings
        // (multiple dispatchers will likely be retrying the same messages)
        // as well as saving the actual message bodies to file and reading those
        // back when it comes time to process the retry i.e. keep as little
        // in memory as possible + share references where we can.
        this.channelName = message.getChannelName().intern();
        this.eventUUID = message.getEventUUID().intern();
        this.eventDispatcher = eventDispatcher;
        this.lastUpdateTime = timestamp;
    }

    public boolean needsMoreTimeToLandInStore() {
        // There's no way it should take more than 10 seconds for
        // event to hit the store (10 seconds longer than it took to
        // hit the dispatcher). Once we go outside that window,
        // then we return false from this function.
        return (System.currentTimeMillis() - timestamp < 10000);
    }

    public int incRetryCount(){
        retryCount++;
        return retryCount;
    }

    public int getRetryCount() { return retryCount; }

    public boolean isExpired() {return System.currentTimeMillis() - timestamp > EXPIRY_TIME_MS; }

    public String getChannelName() {
        return channelName;
    }

    public String getEventUUID() {
        return eventUUID;
    }

    public EventDispatcher getEventDispatcher() { return eventDispatcher; }

    public void clear() { this.eventDispatcher = null; }

    public long getTimestamp() { return timestamp; }

    public long getLastUpdateTime() { return lastUpdateTime; }
    public long updateLastUpdateTime() {
        lastUpdateTime = System.currentTimeMillis();
        return lastUpdateTime;
    }
}
