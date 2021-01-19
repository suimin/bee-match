package com.bee.match.match.trader;

import com.bee.match.match.TradeMatch;
import com.bee.match.match.def.OriginOrder;
import com.bee.match.match.in.MessageIn;

import java.util.List;

public class MessageInWorker {

    private final MessageIn messageIn;
    private final TradeMatch tradeMatch;

    public MessageInWorker(MessageIn messageIn, TradeMatch tradeMatch) {
        this.messageIn = messageIn;
        this.tradeMatch = tradeMatch;
    }

    public void start() {
        new Thread(() -> {
            while (tradeMatch.isReady()) {
                List<OriginOrder> originOrders = messageIn.get();
                if (originOrders != null) {
                    for (OriginOrder originOrder : originOrders) {
                        tradeMatch.trade(originOrder);
                    }
                }
            }
        }).start();
    }
}
