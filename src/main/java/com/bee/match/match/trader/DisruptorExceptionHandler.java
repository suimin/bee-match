package com.bee.match.match.trader;

import com.lmax.disruptor.ExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DisruptorExceptionHandler implements ExceptionHandler<Object> {

    private final Logger logger = LoggerFactory.getLogger(DisruptorExceptionHandler.class);

    @Override
    public void handleEventException(Throwable ex, long sequence, Object event) {
        logger.error("process data error sequence ==[{}] event==[{}] ,ex ==[{}]", sequence, event.toString(), ex.getMessage());
    }

    @Override
    public void handleOnStartException(Throwable ex) {
        logger.error("start disruptor error ==[{}]!", ex.getMessage());
    }

    @Override
    public void handleOnShutdownException(Throwable ex) {
        logger.error("shutdown disruptor error ==[{}]!", ex.getMessage());
    }

}
