package com.bee.match.entity;

import lombok.Data;

import java.io.Serializable;
import java.math.BigDecimal;

@Data
public class ExchangeTrade implements Serializable {
    private String symbol;
    private BigDecimal price;
    private BigDecimal amount;
    private BigDecimal buyTurnover;
    private BigDecimal sellTurnover;
    private ExchangeOrderDirection direction;
    private String buyOrderId;
    private String sellOrderId;
    private Long time;
}