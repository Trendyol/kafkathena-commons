package com.trendyol.mpc.kafkathena.commons.interceptor;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.MDC;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;

@Slf4j
public class KSProducerInterceptor implements ProducerInterceptor<String, Object> {
    public static final String X_CORRELATION_ID = "x-correlation-id";

    @Override
    public ProducerRecord<String, Object> onSend(ProducerRecord<String, Object> payload) {
        setCorrelationId(payload);
        return payload;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        log.debug("{} onAcknowledgement", getClass().getSimpleName());
    }

    @Override
    public void close() {
        log.debug("{} close", getClass().getSimpleName());
    }

    @Override
    public void configure(Map<String, ?> configs) {
        log.debug("{} configure {}", getClass().getSimpleName(), configs);
    }

    private void setCorrelationId(ProducerRecord<String, Object> payload) {
        String correlationId = MDC.get(X_CORRELATION_ID);
        if (StringUtils.isBlank(correlationId)) {
            correlationId = UUID.randomUUID().toString();
        }
        payload.headers().add(X_CORRELATION_ID, correlationId.getBytes(StandardCharsets.UTF_8));
    }
}
