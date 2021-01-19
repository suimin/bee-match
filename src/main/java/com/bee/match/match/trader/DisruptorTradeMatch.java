package com.bee.match.match.trader;

import com.bee.match.entity.ExchangeOrder;
import com.bee.match.entity.ExchangeOrderDirection;
import com.bee.match.entity.ExchangeTrade;
import com.bee.match.entity.TradePlate;
import com.bee.match.match.TradeMatch;
import com.bee.match.match.def.ExchangeEvent;
import com.bee.match.match.def.ExchangeOrderWrapper;
import com.bee.match.match.def.OriginOrder;
import com.bee.match.match.in.MessageIn;
import com.bee.match.match.out.MessageOut;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.WorkHandler;

import java.util.List;

public class DisruptorTradeMatch extends TradeMatch {

    //disruptor引擎
    private DisruptorEngine<ExchangeOrderWrapper> disruptor;
    //买方盘口变化
    private ExchangeTradePlateHandler buyPlateHandler;
    //卖方盘口变化
    private ExchangeTradePlateHandler sellPlateHandler;
    //消息发送代理
    private final MessageOut messageOut;
    private final MessageIn messageIn;

    public DisruptorTradeMatch(String symbol, MessageOut messageOut, MessageIn messageIn) {
        super(symbol);
        this.messageOut = messageOut;
        this.messageIn = messageIn;
        initDisruptor();
        initPlateThread();
    }

    private void initDisruptor() {
        // TODO 冷启动， 热启动, 暂停交易， 重启交易
        DisruptorEngine.Builder<ExchangeOrderWrapper> engineBuilder = DisruptorEngine.builder();
        engineBuilder.eventFactory(ExchangeOrderWrapper::new).threadName(getSymbol() + "-MATCH");
        engineBuilder.workerHandler(event -> {
            if (event.getExchangeEvent() == ExchangeEvent.TRADE) {
                trade(event);
            } else if (event.getExchangeEvent() == ExchangeEvent.CANCEL) {
                cancel(event);
            }
        });
        engineBuilder.processHandler(new ExchangeTradeHandler(messageOut), new ExchangeTradeHandler(messageOut), new ExchangeOrderHandler(messageOut), new ExchangeOrderHandler(messageOut));
        disruptor = engineBuilder.start();
    }

    private void initPlateThread() {
        buyPlateHandler = new ExchangeTradePlateHandler(getTradePlate(ExchangeOrderDirection.BUY), messageOut);
        sellPlateHandler = new ExchangeTradePlateHandler(getTradePlate(ExchangeOrderDirection.SELL), messageOut);
        ExchangeTradePlateWorker tradePlateWorker = new ExchangeTradePlateWorker(buyPlateHandler, sellPlateHandler);
        tradePlateWorker.start();
    }


    @Override
    public void doStart() {
        // TODO init data
        new MessageInWorker(messageIn, this).start();
    }

    @Override
    public void trade(OriginOrder order) {
        disruptor.push(TRADE_TRANSLATOR, order);
    }

    @Override
    public void cancel(OriginOrder order) {
        disruptor.push(CANCEL_TRANSLATOR, order);
    }

    @Override
    protected void handleExchangeTrade(ExchangeOrder exchangeOrder, List<ExchangeTrade> trades) {
        ExchangeOrderWrapper wrapper = (ExchangeOrderWrapper) exchangeOrder;
        if (trades != null && trades.size() > 0) {
            wrapper.setCompleteTrades(trades);
        }
    }

    @Override
    protected void handleCompletedOrder(ExchangeOrder exchangeOrder, List<ExchangeOrder> orders) {
        ExchangeOrderWrapper wrapper = (ExchangeOrderWrapper) exchangeOrder;
        if (orders != null && orders.size() > 0) {
            wrapper.setCompleteOrders(orders);
        }
    }

    @Override
    protected void handleTradePlate(TradePlate plate) {
        ExchangeOrderDirection direction = plate.getDirection();
        if (direction == ExchangeOrderDirection.BUY) {
            buyPlateHandler.ready();
        } else if (direction == ExchangeOrderDirection.SELL) {
            sellPlateHandler.ready();
        }
    }

    private final EventTranslatorOneArg<ExchangeOrderWrapper, OriginOrder> TRADE_TRANSLATOR = (order, sequence, in) -> {
        clearComplete(order);
        in.convert(order);
        order.setExchangeEvent(ExchangeEvent.TRADE);
    };

    private final EventTranslatorOneArg<ExchangeOrderWrapper, OriginOrder> CANCEL_TRANSLATOR = (order, sequence, in) -> {
        clearComplete(order);
        in.convert(order);
        order.setExchangeEvent(ExchangeEvent.CANCEL);
    };

    private void clearComplete(ExchangeOrderWrapper exchangeOrder) {
        exchangeOrder.setCompleteTrades(null);
        exchangeOrder.setCompleteOrders(null);
    }
}
