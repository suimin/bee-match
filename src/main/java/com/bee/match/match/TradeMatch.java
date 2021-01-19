package com.bee.match.match;

import com.bee.match.entity.*;
import com.bee.match.exception.CloneFailureException;
import com.bee.match.exception.IllegalStatusException;
import com.bee.match.match.def.OriginOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.*;

// 当前实现为单线程撮合
public abstract class TradeMatch {
    private final String symbol;
    //交易币种的精度
    private final int coinScale = 4;
    private final Logger logger = LoggerFactory.getLogger(TradeMatch.class);
    //买入限价订单链表，价格从高到低排列
    private TreeMap<BigDecimal, MergeOrder> buyLimitPriceQueue;
    //卖出限价订单链表，价格从低到高排列
    private TreeMap<BigDecimal, MergeOrder> sellLimitPriceQueue;
    //买入市价订单链表，按时间从小到大排序
    private LinkedList<ExchangeOrder> buyMarketQueue;
    //卖出市价订单链表，按时间从小到大排序
    private LinkedList<ExchangeOrder> sellMarketQueue;
    //卖盘盘口信息
    private TradePlate sellTradePlate;
    //买盘盘口信息
    private TradePlate buyTradePlate;
    //是否准备完成
    private boolean ready = false;

    public TradeMatch(String symbol) {
        this.symbol = symbol;
        init();
    }

    private void init() {
        logger.info("init CoinTrader for symbol {}", symbol);
        //买单队列价格降序排列
        buyLimitPriceQueue = new TreeMap<>(Comparator.reverseOrder());
        //卖单队列价格升序排列
        sellLimitPriceQueue = new TreeMap<>(Comparator.naturalOrder());
        buyMarketQueue = new LinkedList<>();
        sellMarketQueue = new LinkedList<>();
        sellTradePlate = new TradePlate(symbol, ExchangeOrderDirection.SELL);
        buyTradePlate = new TradePlate(symbol, ExchangeOrderDirection.BUY);
    }

    public void start() {
        doStart();
        ready = true;
    }

    public abstract void doStart();

    public abstract void trade(OriginOrder order);

    public abstract void cancel(OriginOrder order);

    protected abstract void handleExchangeTrade(ExchangeOrder exchangeOrder, List<ExchangeTrade> trades);

    protected abstract void handleCompletedOrder(ExchangeOrder exchangeOrder, List<ExchangeOrder> orders);

    protected abstract void handleTradePlate(TradePlate plate);

    public String getSymbol() {
        return symbol;
    }

    public TradePlate getTradePlate(ExchangeOrderDirection direction) {
        if (direction == ExchangeOrderDirection.BUY) {
            return buyTradePlate;
        } else {
            return sellTradePlate;
        }
    }

    public ExchangeOrder findOrder(String orderId, ExchangeOrderType type, ExchangeOrderDirection direction) {
        if (type == ExchangeOrderType.MARKET_PRICE) {
            LinkedList<ExchangeOrder> list;
            if (direction == ExchangeOrderDirection.BUY) {
                list = this.buyMarketQueue;
            } else {
                list = this.sellMarketQueue;
            }
            Iterator<ExchangeOrder> orderIterator = list.iterator();
            while (orderIterator.hasNext()) {
                ExchangeOrder order = orderIterator.next();
                if (order.getOrderId().equalsIgnoreCase(orderId)) {
                    return order;
                }
            }
        } else {
            TreeMap<BigDecimal, MergeOrder> list;
            if (direction == ExchangeOrderDirection.BUY) {
                list = this.buyLimitPriceQueue;
            } else {
                list = this.sellLimitPriceQueue;
            }
            Iterator<Map.Entry<BigDecimal, MergeOrder>> mergeOrderIterator = list.entrySet().iterator();
            while (mergeOrderIterator.hasNext()) {
                Map.Entry<BigDecimal, MergeOrder> entry = mergeOrderIterator.next();
                MergeOrder mergeOrder = entry.getValue();
                Iterator<ExchangeOrder> orderIterator = mergeOrder.iterator();
                while ((orderIterator.hasNext())) {
                    ExchangeOrder order = orderIterator.next();
                    if (order.getOrderId().equalsIgnoreCase(orderId)) {
                        return order;
                    }
                }
            }
        }
        return null;
    }

    public TreeMap<BigDecimal, MergeOrder> getBuyLimitPriceQueue() {
        return buyLimitPriceQueue;
    }

    public LinkedList<ExchangeOrder> getBuyMarketQueue() {
        return buyMarketQueue;
    }

    public TreeMap<BigDecimal, MergeOrder> getSellLimitPriceQueue() {
        return sellLimitPriceQueue;
    }

    public LinkedList<ExchangeOrder> getSellMarketQueue() {
        return sellMarketQueue;
    }

    public boolean isReady() {
        return this.ready;
    }

    public int getLimitPriceOrderCount(ExchangeOrderDirection direction) {
        int count = 0;
        TreeMap<BigDecimal, MergeOrder> queue = direction == ExchangeOrderDirection.BUY ? buyLimitPriceQueue : sellLimitPriceQueue;
        Iterator<Map.Entry<BigDecimal, MergeOrder>> mergeOrderIterator = queue.entrySet().iterator();
        while (mergeOrderIterator.hasNext()) {
            Map.Entry<BigDecimal, MergeOrder> entry = mergeOrderIterator.next();
            MergeOrder mergeOrder = entry.getValue();
            count += mergeOrder.size();
        }
        return count;
    }

    //增加限价订单到队列，买入单按从价格高到低排，卖出单按价格从低到高排
    private void addLimitPriceOrder(ExchangeOrder exchangeOrder) {
        if (exchangeOrder.getType() != ExchangeOrderType.LIMIT_PRICE) {
            throw new IllegalStatusException("error order status");
        }
        ExchangeOrder cloneOrder;
        try {
            cloneOrder = (ExchangeOrder) exchangeOrder.clone();
        } catch (CloneNotSupportedException e) {
            throw new CloneFailureException("clone error, orderId : " + exchangeOrder.getOrderId());
        }
        TreeMap<BigDecimal, MergeOrder> list;
        if (exchangeOrder.getDirection() == ExchangeOrderDirection.BUY) {
            list = buyLimitPriceQueue;
            buyTradePlate.add(cloneOrder);
            handleTradePlate(buyTradePlate);
        } else {
            list = sellLimitPriceQueue;
            sellTradePlate.add(cloneOrder);
            handleTradePlate(sellTradePlate);
        }
        MergeOrder mergeOrder = list.get(cloneOrder.getPrice());
        if (mergeOrder == null) {
            mergeOrder = new MergeOrder();
            mergeOrder.add(cloneOrder);
            list.put(exchangeOrder.getPrice(), mergeOrder);
        } else {
            mergeOrder.add(cloneOrder);
        }
    }

    public void addMarketPriceOrder(ExchangeOrder exchangeOrder) {
        if (exchangeOrder.getType() != ExchangeOrderType.MARKET_PRICE) {
            throw new IllegalStatusException("error order status");
        }
        ExchangeOrder cloneOrder;
        try {
            cloneOrder = (ExchangeOrder) exchangeOrder.clone();
        } catch (CloneNotSupportedException e) {
            throw new CloneFailureException("clone error, orderId : " + exchangeOrder.getOrderId());
        }
        LinkedList<ExchangeOrder> list = exchangeOrder.getDirection() == ExchangeOrderDirection.BUY ? buyMarketQueue : sellMarketQueue;
        list.addLast(cloneOrder);
    }

    //市价委托单与限价对手单列表交易
    private void matchMarketPriceWithLPList(TreeMap<BigDecimal, MergeOrder> lpList, ExchangeOrder focusedOrder) {
        List<ExchangeTrade> exchangeTrades = new ArrayList<>();
        List<ExchangeOrder> completedOrders = new ArrayList<>();
        Iterator<Map.Entry<BigDecimal, MergeOrder>> mergeOrderIterator = lpList.entrySet().iterator();
        boolean exitLoop = false;
        while (!exitLoop && mergeOrderIterator.hasNext()) {
            Map.Entry<BigDecimal, MergeOrder> entry = mergeOrderIterator.next();
            MergeOrder mergeOrder = entry.getValue();
            Iterator<ExchangeOrder> orderIterator = mergeOrder.iterator();
            while (orderIterator.hasNext()) {
                ExchangeOrder matchOrder = orderIterator.next();
                //处理匹配
                ExchangeTrade trade = processMatch(focusedOrder, matchOrder);
                if (trade != null) {
                    exchangeTrades.add(trade);
                }
                //判断匹配单是否完成
                if (matchOrder.isCompleted()) {
                    //当前匹配的订单完成交易，删除该订单
                    orderIterator.remove();
                    completedOrders.add(matchOrder);
                }
                //判断焦点订单是否完成
                if (focusedOrder.isCompleted()) {
                    completedOrders.add(focusedOrder);
                    //退出循环
                    exitLoop = true;
                    break;
                }
            }
            if (mergeOrder.size() == 0) {
                mergeOrderIterator.remove();
            }
        }
        //如果还没有交易完，订单压入列表中,市价买单按成交量算
        if (focusedOrder.getDirection() == ExchangeOrderDirection.SELL && focusedOrder.getTradedAmount().compareTo(focusedOrder.getAmount()) < 0
                || focusedOrder.getDirection() == ExchangeOrderDirection.BUY && focusedOrder.getTurnover().compareTo(focusedOrder.getAmount()) < 0) {
            addMarketPriceOrder(focusedOrder);
        }
        //每个订单的匹配批量推送
        if (exchangeTrades.size() > 0) {
            handleExchangeTrade(focusedOrder, exchangeTrades);
        }
        if (completedOrders.size() > 0) {
            handleCompletedOrder(focusedOrder, completedOrders);
            handleTradePlate(focusedOrder.getDirection() == ExchangeOrderDirection.BUY ? sellTradePlate : buyTradePlate);
        }
    }

    //限价委托单与限价队列匹配
    private void matchLimitPriceWithLPMPList(TreeMap<BigDecimal, MergeOrder> lpList, LinkedList<ExchangeOrder> mpList, ExchangeOrder focusedOrder) {
        List<ExchangeTrade> exchangeTrades = new ArrayList<>();
        List<ExchangeOrder> completedOrders = new ArrayList<>();
        Iterator<Map.Entry<BigDecimal, MergeOrder>> mergeOrderIterator = lpList.entrySet().iterator();
        boolean exitLoop = false;
        while (!exitLoop && mergeOrderIterator.hasNext()) {
            Map.Entry<BigDecimal, MergeOrder> entry = mergeOrderIterator.next();
            MergeOrder mergeOrder = entry.getValue();
            Iterator<ExchangeOrder> orderIterator = mergeOrder.iterator();
            //买入单需要匹配的价格不大于委托价，否则退出
            if (focusedOrder.getDirection() == ExchangeOrderDirection.BUY && mergeOrder.getPrice().compareTo(focusedOrder.getPrice()) > 0) {
                break;
            }
            //卖出单需要匹配的价格不小于委托价，否则退出
            if (focusedOrder.getDirection() == ExchangeOrderDirection.SELL && mergeOrder.getPrice().compareTo(focusedOrder.getPrice()) < 0) {
                break;
            }
            while (orderIterator.hasNext()) {
                ExchangeOrder matchOrder = orderIterator.next();
                //处理匹配
                ExchangeTrade trade = processMatch(focusedOrder, matchOrder);
                exchangeTrades.add(trade);
                //判断匹配单是否完成
                if (matchOrder.isCompleted()) {
                    //当前匹配的订单完成交易，删除该订单
                    orderIterator.remove();
                    completedOrders.add(matchOrder);
                }
                //判断交易单是否完成
                if (focusedOrder.isCompleted()) {
                    //交易完成
                    completedOrders.add(focusedOrder);
                    //退出循环
                    exitLoop = true;
                    break;
                }
            }
            if (mergeOrder.size() == 0) {
                mergeOrderIterator.remove();
            }
        }
        //如果还没有交易完，继续撮合市价单
        if (focusedOrder.getAmount().compareTo(focusedOrder.getTradedAmount()) > 0) {
            //后与市价单交易
            Iterator<ExchangeOrder> iterator = mpList.iterator();
            while (iterator.hasNext()) {
                ExchangeOrder matchOrder = iterator.next();
                ExchangeTrade trade = processMatch(focusedOrder, matchOrder);
                if (trade != null) {
                    exchangeTrades.add(trade);
                }
                //判断匹配单是否完成，市价单amount为成交量
                if (matchOrder.isCompleted()) {
                    iterator.remove();
                    completedOrders.add(matchOrder);
                }
                //判断吃单是否完成，判断成交量是否完成
                if (focusedOrder.isCompleted()) {
                    //交易完成
                    completedOrders.add(focusedOrder);
                    //退出循环
                    break;
                }
            }
            //如果还没有交易完，订单压入列表中
            if (focusedOrder.getTradedAmount().compareTo(focusedOrder.getAmount()) < 0) {
                addLimitPriceOrder(focusedOrder);
            }
            //每个订单的匹配批量推送
            handleExchangeTrade(focusedOrder, exchangeTrades);
            handleCompletedOrder(focusedOrder, completedOrders);
        }
        if (exchangeTrades.size() > 0) {
            handleExchangeTrade(focusedOrder, exchangeTrades);
        }
        if (completedOrders.size() > 0) {
            handleCompletedOrder(focusedOrder, completedOrders);
            handleTradePlate(focusedOrder.getDirection() == ExchangeOrderDirection.BUY ? sellTradePlate : buyTradePlate);
        }
    }

    //处理两个匹配的委托订单
    private ExchangeTrade processMatch(ExchangeOrder focusedOrder, ExchangeOrder matchOrder) {
        //需要交易的数量，成交量,成交价，可用数量
        BigDecimal needAmount, dealPrice, availAmount;
        //如果匹配单是限价单，则以其价格为成交价
        if (matchOrder.getType() == ExchangeOrderType.LIMIT_PRICE) {
            dealPrice = matchOrder.getPrice();
        } else {
            dealPrice = focusedOrder.getPrice();
        }
        //成交价必须大于0
        if (dealPrice.compareTo(BigDecimal.ZERO) <= 0) {
            return null;
        }
        needAmount = calculateTradedAmount(focusedOrder, dealPrice);
        availAmount = calculateTradedAmount(matchOrder, dealPrice);
        //计算成交量
        BigDecimal tradedAmount = (availAmount.compareTo(needAmount) >= 0 ? needAmount : availAmount);
        //如果成交额为0说明剩余额度无法成交，退出
        if (tradedAmount.compareTo(BigDecimal.ZERO) == 0) {
            return null;
        }

        //计算成交额,成交额要保留足够精度
        BigDecimal turnover = tradedAmount.multiply(dealPrice);
        matchOrder.setTradedAmount(matchOrder.getTradedAmount().add(tradedAmount));
        matchOrder.setTurnover(matchOrder.getTurnover().add(turnover));
        focusedOrder.setTradedAmount(focusedOrder.getTradedAmount().add(tradedAmount));
        focusedOrder.setTurnover(focusedOrder.getTurnover().add(turnover));

        //创建成交记录
        ExchangeTrade exchangeTrade = new ExchangeTrade();
        exchangeTrade.setSymbol(symbol);
        exchangeTrade.setAmount(tradedAmount);
        exchangeTrade.setDirection(focusedOrder.getDirection());
        exchangeTrade.setPrice(dealPrice);
        exchangeTrade.setBuyTurnover(turnover);
        exchangeTrade.setSellTurnover(turnover);
        //校正市价单剩余成交额
        if (ExchangeOrderType.MARKET_PRICE == focusedOrder.getType() && focusedOrder.getDirection() == ExchangeOrderDirection.BUY) {
            BigDecimal adjustTurnover = adjustMarketOrderTurnover(focusedOrder, dealPrice);
            exchangeTrade.setBuyTurnover(turnover.add(adjustTurnover));
        } else if (ExchangeOrderType.MARKET_PRICE == matchOrder.getType() && matchOrder.getDirection() == ExchangeOrderDirection.BUY) {
            BigDecimal adjustTurnover = adjustMarketOrderTurnover(matchOrder, dealPrice);
            exchangeTrade.setBuyTurnover(turnover.add(adjustTurnover));
        }

        if (focusedOrder.getDirection() == ExchangeOrderDirection.BUY) {
            exchangeTrade.setBuyOrderId(focusedOrder.getOrderId());
            exchangeTrade.setSellOrderId(matchOrder.getOrderId());
        } else {
            exchangeTrade.setBuyOrderId(matchOrder.getOrderId());
            exchangeTrade.setSellOrderId(focusedOrder.getOrderId());
        }

        exchangeTrade.setTime(Calendar.getInstance().getTimeInMillis());
        if (matchOrder.getType() == ExchangeOrderType.LIMIT_PRICE) {
            if (matchOrder.getDirection() == ExchangeOrderDirection.BUY) {
                buyTradePlate.remove(matchOrder, tradedAmount);
                handleTradePlate(buyTradePlate);
            } else {
                sellTradePlate.remove(matchOrder, tradedAmount);
                handleTradePlate(sellTradePlate);
            }
        }
        return exchangeTrade;
    }

    //计算委托单剩余可成交的数量
    private BigDecimal calculateTradedAmount(ExchangeOrder order, BigDecimal dealPrice) {
        if (order.getDirection() == ExchangeOrderDirection.BUY && order.getType() == ExchangeOrderType.MARKET_PRICE) {
            //剩余成交量
            BigDecimal leftTurnover = order.getAmount().subtract(order.getTurnover());
            return leftTurnover.divide(dealPrice, coinScale, BigDecimal.ROUND_DOWN);
        } else {
            return order.getAmount().subtract(order.getTradedAmount());
        }
    }

    //调整市价单剩余成交额，当剩余成交额不足时设置订单完成
    private BigDecimal adjustMarketOrderTurnover(ExchangeOrder order, BigDecimal dealPrice) {
        if (order.getDirection() == ExchangeOrderDirection.BUY && order.getType() == ExchangeOrderType.MARKET_PRICE) {
            BigDecimal leftTurnover = order.getAmount().subtract(order.getTurnover());
            if (leftTurnover.divide(dealPrice, coinScale, BigDecimal.ROUND_DOWN)
                    .compareTo(BigDecimal.ZERO) == 0) {
                order.setTurnover(order.getAmount());
                return leftTurnover;
            }
        }
        return BigDecimal.ZERO;
    }

    private void onRemoveOrder(ExchangeOrder order) {
        if (order.getType() == ExchangeOrderType.LIMIT_PRICE) {
            if (order.getDirection() == ExchangeOrderDirection.BUY) {
                buyTradePlate.remove(order);
                handleTradePlate(buyTradePlate);
            } else {
                sellTradePlate.remove(order);
                handleTradePlate(sellTradePlate);
            }
        }
    }

    protected void trade(ExchangeOrder exchangeOrder) {
        logger.info("trade start, symbol[{}] desc[{}] orderId[{}] amount[{}]", exchangeOrder.getSymbol(), exchangeOrder.getDirection().name(), exchangeOrder.getOrderId(), exchangeOrder.getAmount());
        if (!symbol.equalsIgnoreCase(exchangeOrder.getSymbol())) {
            throw new IllegalStatusException("unsupported symbol, order symbol " + exchangeOrder.getSymbol() + ", expect symbol " + getSymbol());
        }
        if (exchangeOrder.getAmount().compareTo(BigDecimal.ZERO) <= 0 || exchangeOrder.getAmount().subtract(exchangeOrder.getTradedAmount()).compareTo(BigDecimal.ZERO) <= 0) {
            throw new IllegalStatusException("unusual order amount, order id " + exchangeOrder.getOrderId() + " amount " + exchangeOrder.getAmount() + " tradeAmount " + exchangeOrder.getTradedAmount());
        }
        if (exchangeOrder.getType() == ExchangeOrderType.LIMIT_PRICE && exchangeOrder.getPrice().compareTo(BigDecimal.ZERO) <= 0) {
            throw new IllegalStatusException("unusual order price, order id " + exchangeOrder.getOrderId() + " price " + exchangeOrder.getPrice());
        }

        TreeMap<BigDecimal, MergeOrder> limitPriceOrderList;
        LinkedList<ExchangeOrder> marketPriceOrderList;
        if (exchangeOrder.getDirection() == ExchangeOrderDirection.BUY) {
            limitPriceOrderList = sellLimitPriceQueue;
            marketPriceOrderList = sellMarketQueue;
        } else {
            limitPriceOrderList = buyLimitPriceQueue;
            marketPriceOrderList = buyMarketQueue;
        }
        if (exchangeOrder.getType() == ExchangeOrderType.MARKET_PRICE) {
            logger.debug("trade market-limit >>, symbol[{}] desc[{}] orderId[{}] amount[{}] traded[{}]", exchangeOrder.getSymbol(), exchangeOrder.getDirection().name(), exchangeOrder.getOrderId(), exchangeOrder.getAmount(), exchangeOrder.getTradedAmount());
            //市价单与限价单交易
            matchMarketPriceWithLPList(limitPriceOrderList, exchangeOrder);
            logger.debug("trade market-limit <<, symbol[{}] desc[{}] orderId[{}] amount[{}] traded[{}]", exchangeOrder.getSymbol(), exchangeOrder.getDirection().name(), exchangeOrder.getOrderId(), exchangeOrder.getAmount(), exchangeOrder.getTradedAmount());
        } else if (exchangeOrder.getType() == ExchangeOrderType.LIMIT_PRICE) {
            logger.debug("trade limit-limit/market >>, symbol[{}] desc[{}] orderId[{}] amount[{}] traded[{}]", exchangeOrder.getSymbol(), exchangeOrder.getDirection().name(), exchangeOrder.getOrderId(), exchangeOrder.getAmount(), exchangeOrder.getTradedAmount());
            //先与限价单交易，后与市价单交易
            matchLimitPriceWithLPMPList(limitPriceOrderList, marketPriceOrderList, exchangeOrder);
            logger.debug("trade limit-limit/market <<, symbol[{}] desc[{}] orderId[{}] amount[{}] traded[{}]", exchangeOrder.getSymbol(), exchangeOrder.getDirection().name(), exchangeOrder.getOrderId(), exchangeOrder.getAmount(), exchangeOrder.getTradedAmount());
        }
    }

    protected void cancel(ExchangeOrder exchangeOrder) {
        if (exchangeOrder.getType() == ExchangeOrderType.MARKET_PRICE) {
            //处理市价单
            Iterator<ExchangeOrder> orderIterator;
            List<ExchangeOrder> list;
            if (exchangeOrder.getDirection() == ExchangeOrderDirection.BUY) {
                list = this.buyMarketQueue;
            } else {
                list = this.sellMarketQueue;
            }
            orderIterator = list.iterator();
            while ((orderIterator.hasNext())) {
                ExchangeOrder order = orderIterator.next();
                if (order.getOrderId().equalsIgnoreCase(exchangeOrder.getOrderId())) {
                    orderIterator.remove();
                    onRemoveOrder(order);
                    break;
                }
            }
        } else {
            //处理限价单
            TreeMap<BigDecimal, MergeOrder> list;
            if (exchangeOrder.getDirection() == ExchangeOrderDirection.BUY) {
                list = this.buyLimitPriceQueue;
            } else {
                list = this.sellLimitPriceQueue;
            }
            MergeOrder mergeOrder = list.get(exchangeOrder.getPrice());
            if (mergeOrder != null) {
                Iterator<ExchangeOrder> orderIterator = mergeOrder.iterator();
                while (orderIterator.hasNext()) {
                    ExchangeOrder order = orderIterator.next();
                    if (order.getOrderId().equalsIgnoreCase(exchangeOrder.getOrderId())) {
                        orderIterator.remove();
                        if (mergeOrder.size() == 0) {
                            list.remove(exchangeOrder.getPrice());
                        }
                        onRemoveOrder(order);
                        break;
                    }
                }
            }
        }
    }
}
