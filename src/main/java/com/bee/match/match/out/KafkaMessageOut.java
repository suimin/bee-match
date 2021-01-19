package com.bee.match.match.out;

import com.bee.match.entity.ExchangeOrder;
import com.bee.match.entity.ExchangeTrade;
import com.bee.match.entity.TradePlate;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.ProducerListener;

import java.util.List;

@Slf4j
public class KafkaMessageOut implements MessageOut {

    private final KafkaTemplate<String, String> quickKafka;

    private final KafkaTemplate<String, String> safeKafka;

    private final ObjectMapper objectMapper = new ObjectMapper();

    public KafkaMessageOut(KafkaTemplate<String, String> quickKafka, KafkaTemplate<String, String> safeKafka, String symbol) {
        this.quickKafka = quickKafka;
        this.safeKafka = safeKafka;
        safeKafka.setProducerListener(new ProducerListener<String, String>() {
            @Override
            public void onError(ProducerRecord<String, String> producerRecord, Exception exception) {
                // TODO 发送失败处理
            }
        });
    }

    @Override
    public void sendTradePlate(TradePlate tradePlate) {
        try {
            quickKafka.send("trade-plate", objectMapper.writeValueAsString(tradePlate));
        } catch (JsonProcessingException e) {
            log.error("sendTradePlate", e);
        }
    }

    @Override
    public void sendExchangeTrade(List<ExchangeTrade> trades) {
        try {
            safeKafka.send("exchange-trade", objectMapper.writeValueAsString(trades));
        } catch (JsonProcessingException e) {
            log.error("sendExchangeTrade", e);
        }
    }

    @Override
    public void sendExchangeOrder(List<ExchangeOrder> orders) {
        try {
            safeKafka.send("exchange-order", objectMapper.writeValueAsString(orders));
        } catch (JsonProcessingException e) {
            log.error("sendExchangeOrder", e);
        }
    }
}
