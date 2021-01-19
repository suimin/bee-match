package com.bee.match.match.trader;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import java.util.concurrent.ThreadFactory;

public class DisruptorEngine<T> {

    private final RingBuffer<T> ringBuffer;

    public DisruptorEngine(RingBuffer<T> ringBuffer) {
        this.ringBuffer = ringBuffer;
    }

    public <E> void push(EventTranslatorOneArg<T, E> translatorOneArg, E event) {
        ringBuffer.publishEvent(translatorOneArg, event);
    }

    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    public static class Builder<T>{

        private WorkHandler<T> worker;
        private WorkHandler<T>[] processList;
        private EventFactory<T> eventFactory;
        private String threadName;

        public Builder<T> workerHandler(WorkHandler<T> worker) {
            this.worker = worker;
            return this;
        }

        public Builder<T> processHandler(WorkHandler<T>... processList) {
            this.processList = processList;
            return this;
        }

        public Builder<T> eventFactory(EventFactory<T> eventFactory) {
            this.eventFactory = eventFactory;
            return this;
        }

        public Builder<T> threadName(String threadName) {
            this.threadName = threadName;
            return this;
        }

        public DisruptorEngine<T> start() {
            if (worker == null) {
                throw new IllegalArgumentException("worker not set");
            }
            if (eventFactory == null) {
                throw new IllegalArgumentException("eventFactory not set");
            }
            ThreadFactory disruptorThreadPool = new ThreadFactoryBuilder().setNameFormat(threadName != null ? threadName : "disruptor").build();
            Disruptor<T> disruptor = new Disruptor<>(eventFactory, 1024 * 1024, disruptorThreadPool,
                    ProducerType.MULTI, new YieldingWaitStrategy());
            disruptor.setDefaultExceptionHandler(new DisruptorExceptionHandler());
            disruptor.handleEventsWithWorkerPool(worker).thenHandleEventsWithWorkerPool(processList);
            RingBuffer<T> ringBuffer = disruptor.start();
            return new DisruptorEngine<>(ringBuffer);
        }
    }
}
