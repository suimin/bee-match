package com.bee.match.entity;

import lombok.Data;

import java.io.Serializable;
import java.math.BigDecimal;

@Data
public class ExchangeOrder implements Serializable, Cloneable {
    //订单ID
    private String orderId;
    //用户ID
    private Long memberId;
    //交易对符号
    private String symbol;
    //币单位
    private String coinSymbol;
    //结算单位
    private String baseSymbol;
    //挂单类型
    private ExchangeOrderType type;
    //买入或卖出量，对于市价买入单表
    private BigDecimal amount = BigDecimal.ZERO;
    //成交量
    private BigDecimal tradedAmount = BigDecimal.ZERO;
    //成交额，对市价买单有用
    private BigDecimal turnover = BigDecimal.ZERO;
    //订单状态
    private ExchangeOrderStatus status;
    //订单方向
    private ExchangeOrderDirection direction;
    //挂单价格
    private BigDecimal price = BigDecimal.ZERO;
    //挂单时间
    private Long time;
    //交易完成时间
    private Long completedTime;
    //取消时间
    private Long canceledTime;

    public boolean isCompleted() {
        if (status != ExchangeOrderStatus.TRADING) {
            return true;
        } else {
            if (type == ExchangeOrderType.MARKET_PRICE && direction == ExchangeOrderDirection.BUY) {
                return amount.compareTo(turnover) <= 0;
            } else {
                return amount.compareTo(tradedAmount) <= 0;
            }
        }
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
}