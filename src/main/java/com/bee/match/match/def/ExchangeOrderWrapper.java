package com.bee.match.match.def;

import com.bee.match.entity.ExchangeOrder;
import com.bee.match.entity.ExchangeTrade;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;

@EqualsAndHashCode(callSuper = false)
@Data
public class ExchangeOrderWrapper extends ExchangeOrder {
    private ExchangeEvent exchangeEvent;
    private List<ExchangeOrder> completeOrders;
    private List<ExchangeTrade> completeTrades;
}
