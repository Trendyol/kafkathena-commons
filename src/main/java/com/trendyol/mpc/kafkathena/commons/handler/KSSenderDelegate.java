package com.trendyol.mpc.kafkathena.commons.handler;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;

public interface KSSenderDelegate {
    <T> void send(String producerName, String topic, String key, T message);

    <T> void send(String producerName, ProducerRecord<String, T> payload);

    void send(String producerName, String topic, String key, Object value, String filterHeaderValue);

    void send(String producerName, String topic, String key, Object value, String filterHeaderKey, String filterHeaderValue);

    KafkaTemplate<String, Object> getProducer(String producerName);

    boolean checkProducer(String producerName);
}
