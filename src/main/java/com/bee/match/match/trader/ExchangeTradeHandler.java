package com.bee.match.match.trader;

import com.bee.match.entity.ExchangeTrade;
import com.bee.match.match.def.ExchangeEvent;
import com.bee.match.match.def.ExchangeOrderWrapper;
import com.bee.match.match.out.MessageOut;
import com.lmax.disruptor.WorkHandler;

import java.util.List;

public class ExchangeTradeHandler implements WorkHandler<ExchangeOrderWrapper> {

    private final MessageOut messageOut;

    public ExchangeTradeHandler(MessageOut messageOut) {
        this.messageOut = messageOut;
    }

    @Override
    public void onEvent(ExchangeOrderWrapper event) throws Exception {
        if (event.getExchangeEvent() == ExchangeEvent.TRADE) {
            List<ExchangeTrade> completeTrades = event.getCompleteTrades();
            if (completeTrades != null) {
                messageOut.sendExchangeTrade(completeTrades);
            }
        }
    }
}
