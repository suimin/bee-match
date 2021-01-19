package com.bee.match.match.out;

import com.bee.match.entity.ExchangeOrder;
import com.bee.match.entity.ExchangeTrade;
import com.bee.match.entity.TradePlate;

import java.util.List;

public interface MessageOut {
    // 发送盘口消息
    void sendTradePlate(TradePlate tradePlate);

    void sendExchangeTrade(List<ExchangeTrade> trades);

    void sendExchangeOrder(List<ExchangeOrder> orders);
}
