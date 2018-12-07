package org.jenkinsci.plugins.ssegateway.sse;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

public class RetryManager {

    private static final Logger LOGGER = Logger.getLogger(RetryManager.class.getName());
    private static RetryManager instance = new RetryManager();
    private ThreadPoolExecutor threadPoolExecutor;
    private AtomicLong messageCount = new AtomicLong();

    private RetryManager(){
        threadPoolExecutor = new ThreadPoolExecutor(20,30, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
    }

    public static RetryManager getInstance() { return instance; }

    public void add(final Retry retry){
        long counter = messageCount.incrementAndGet();
        if (counter % 1000 == 0){
            LOGGER.log(Level.INFO, "Retry manager queue size: " + threadPoolExecutor.getQueue().size());
            messageCount.set(0);
        }
        threadPoolExecutor.submit(new Runnable() {
            @Override
            public void run() {
                try{
                    Thread.currentThread().setName("EventDispatcher [" + this.hashCode() + "] " + System.currentTimeMillis() + " - " + retry.getEventDispatcher().getCreationDate());

                    if (System.currentTimeMillis() - retry.getLastUpdateTime() < 100) {
                        Thread.sleep(100);
                    }

                    EventDispatcher dispatcher = retry.getEventDispatcher();
                    synchronized (dispatcher) {
                        dispatcher.processRetry(retry);
                    }

                    Thread.currentThread().setName("Idle RetryManager thread");
                } catch (Exception e){
                    LOGGER.log(Level.SEVERE, "Failed to handle retry: " + e);
                    e.printStackTrace();
                }
            }
        });
    }

}
