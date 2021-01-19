package com.bee.match.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.kafka.DefaultKafkaProducerFactoryCustomizer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.kafka.support.converter.RecordMessageConverter;

import java.util.Map;

@Configuration
public class KafkaConfig {

    @Autowired
    private KafkaProperties properties;

    @Bean("quicklyKafkaTemplate")
    public KafkaTemplate<?, ?> kafkaTemplate(@Qualifier("quicklyKafkaFactory") ProducerFactory<Object, Object> kafkaProducerFactory,
                                             ProducerListener<Object, Object> kafkaProducerListener,
                                             ObjectProvider<RecordMessageConverter> messageConverter) {
        KafkaTemplate<Object, Object> kafkaTemplate = new KafkaTemplate<>(kafkaProducerFactory);
        messageConverter.ifUnique(kafkaTemplate::setMessageConverter);
        kafkaTemplate.setProducerListener(kafkaProducerListener);
        kafkaTemplate.setDefaultTopic(this.properties.getTemplate().getDefaultTopic());
        return kafkaTemplate;
    }

    @Bean("quicklyKafkaFactory")
    public ProducerFactory<?, ?> kafkaProducerFactory(
            ObjectProvider<DefaultKafkaProducerFactoryCustomizer> customizers) {
        Map<String, Object> producerProperties = this.properties.buildProducerProperties();
        producerProperties.put(ProducerConfig.ACKS_CONFIG, 1);
        // Batch size
        producerProperties.put(ProducerConfig.BATCH_SIZE_CONFIG, 1024 * 4);
        // 最慢100ms发送出去
        producerProperties.put(ProducerConfig.LINGER_MS_CONFIG, 100);
        // 缓冲区16M
        producerProperties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 16 * 1024 * 1024);
        // 重试次数
        producerProperties.put(ProducerConfig.RETRIES_CONFIG, 1);
        // 重试间隔300ms
        producerProperties.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 300);
        DefaultKafkaProducerFactory<?, ?> factory = new DefaultKafkaProducerFactory<>(producerProperties);
        String transactionIdPrefix = this.properties.getProducer().getTransactionIdPrefix();
        if (transactionIdPrefix != null) {
            factory.setTransactionIdPrefix(transactionIdPrefix);
        }
        customizers.orderedStream().forEach((customizer) -> customizer.customize(factory));
        return factory;
    }

    @Bean("safeKafkaTemplate")
    public KafkaTemplate<?, ?> kafkaTemplateSafe(@Qualifier("safeKafkaFactory") ProducerFactory<Object, Object> kafkaProducerFactory,
                                                 ProducerListener<Object, Object> kafkaProducerListener,
                                                 ObjectProvider<RecordMessageConverter> messageConverter) {
        KafkaTemplate<Object, Object> kafkaTemplate = new KafkaTemplate<>(kafkaProducerFactory);
        messageConverter.ifUnique(kafkaTemplate::setMessageConverter);
        kafkaTemplate.setProducerListener(kafkaProducerListener);
        kafkaTemplate.setDefaultTopic(this.properties.getTemplate().getDefaultTopic());
        return kafkaTemplate;
    }

    @Bean("safeKafkaFactory")
    public ProducerFactory<?, ?> kafkaProducerFactorySafe(
            ObjectProvider<DefaultKafkaProducerFactoryCustomizer> customizers) {
        Map<String, Object> producerProperties = this.properties.buildProducerProperties();
        producerProperties.put(ProducerConfig.ACKS_CONFIG, -1);
        // Batch size
        producerProperties.put(ProducerConfig.BATCH_SIZE_CONFIG, 1024 * 16);
        // 最慢100ms发送出去
        producerProperties.put(ProducerConfig.LINGER_MS_CONFIG, 100);
        // 缓冲区64M
        producerProperties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 64 * 1024 * 1024);
        // 重试次数
        producerProperties.put(ProducerConfig.RETRIES_CONFIG, 3);
        // 重试间隔300ms
        producerProperties.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 500);
        DefaultKafkaProducerFactory<?, ?> factory = new DefaultKafkaProducerFactory<>(producerProperties);
        String transactionIdPrefix = this.properties.getProducer().getTransactionIdPrefix();
        if (transactionIdPrefix != null) {
            factory.setTransactionIdPrefix(transactionIdPrefix);
        }
        customizers.orderedStream().forEach((customizer) -> customizer.customize(factory));
        return factory;
    }
}
