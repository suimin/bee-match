package com.bee.match.match.trader;

import com.bee.match.entity.TradePlate;
import com.bee.match.match.out.MessageOut;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

public class ExchangeTradePlateHandler implements Runnable {

    private ExchangeTradePlateWorker worker;
    private final AtomicBoolean plateChange = new AtomicBoolean(false);
    private final TradePlate tradePlate;
    private static final long PARK_NANOS = 100000000;//100毫秒

    private final MessageOut messageOut;

    public ExchangeTradePlateHandler(TradePlate tradePlate, MessageOut messageOut) {
        this.tradePlate = tradePlate;
        this.messageOut = messageOut;
    }

    public void setWorker(ExchangeTradePlateWorker worker) {
        this.worker = worker;
    }

    public void ready() {
        plateChange.compareAndSet(false, true);
    }

    @Override
    public void run() {
        while (worker.isRunning()) {
            if (plateChange.compareAndSet(true, false)) {
                messageOut.sendTradePlate(tradePlate);
            } else {
                LockSupport.parkNanos(PARK_NANOS);
            }
        }
    }
}
