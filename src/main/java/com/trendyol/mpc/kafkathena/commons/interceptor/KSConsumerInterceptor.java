package com.trendyol.mpc.kafkathena.commons.interceptor;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.slf4j.MDC;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;

@Slf4j
public class KSConsumerInterceptor implements ConsumerInterceptor<String, String> {
    public static final String X_CORRELATION_ID = "x-correlation-id";

    @Override
    public ConsumerRecords<String, String> onConsume(ConsumerRecords<String, String> consumerRecords) {
        ConsumerRecord<String, String> payload = consumerRecords.iterator().next();
        setCorrelationId(payload);
        return consumerRecords;
    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> map) {
        log.debug("{} onCommit {}", getClass().getSimpleName(), map);
    }

    @Override
    public void close() {
        log.debug("{} close", getClass().getSimpleName());
    }

    @Override
    public void configure(Map<String, ?> map) {
        log.debug("{} configure {}", getClass().getSimpleName(), map);
    }

    public void setCorrelationId(ConsumerRecord<String, String> payload) {
        Iterable<Header> correlationIdHeaderIterable = payload.headers().headers(X_CORRELATION_ID);
        String correlationId = "";
        if (correlationIdHeaderIterable.iterator().hasNext()) {
            Header header = correlationIdHeaderIterable.iterator().next();
            correlationId = new String(header.value(), StandardCharsets.UTF_8);
        } else {
            correlationId = UUID.randomUUID().toString();
            payload.headers().add(X_CORRELATION_ID, correlationId.getBytes(StandardCharsets.UTF_8));
        }

        MDC.put(X_CORRELATION_ID, correlationId);
    }

}
