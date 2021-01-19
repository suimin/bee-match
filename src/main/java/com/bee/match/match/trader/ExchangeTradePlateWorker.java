package com.bee.match.match.trader;

import java.util.concurrent.atomic.AtomicBoolean;

public class ExchangeTradePlateWorker {

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final ExchangeTradePlateHandler[] handlerGroup;

    public ExchangeTradePlateWorker(ExchangeTradePlateHandler... handlerGroup) {
        this.handlerGroup = handlerGroup;
    }

    public boolean isRunning() {
        return running.get();
    }

    public void start() {
        if (!running.compareAndSet(false, true)) {
            throw new IllegalStateException("Thread is already running");
        }
        for (ExchangeTradePlateHandler handler : handlerGroup) {
            handler.setWorker(this);
            Thread thread = new Thread(handler);
            thread.setName("trade-plate-thread");
            thread.start();
        }
    }
}
