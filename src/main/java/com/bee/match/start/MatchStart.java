package com.bee.match.start;

import com.bee.match.match.in.KafkaMessageIn;
import com.bee.match.match.in.MessageIn;
import com.bee.match.match.trader.DisruptorTradeMatch;
import com.bee.match.match.TradeMatch;
import com.bee.match.match.out.KafkaMessageOut;
import com.bee.match.match.out.MessageOut;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class MatchStart implements ApplicationListener<ApplicationStartedEvent> {

    @Autowired
    @Qualifier("quicklyKafkaTemplate")
    private KafkaTemplate<String, String> quicklyTemplate;
    @Autowired
    @Qualifier("safeKafkaTemplate")
    private KafkaTemplate<String, String> safeKafkaTemplate;

    @Override
    public void onApplicationEvent(ApplicationStartedEvent applicationStartedEvent) {
        log.info("match starting ...");
        start();
        log.info("match start finish.");
    }

    private void start() {
        MessageOut messageOut = new KafkaMessageOut(quicklyTemplate, safeKafkaTemplate, "BTC");
        MessageIn messageIn = new KafkaMessageIn();
        TradeMatch match = new DisruptorTradeMatch("BTC", messageOut, messageIn);
        match.start();
    }
}