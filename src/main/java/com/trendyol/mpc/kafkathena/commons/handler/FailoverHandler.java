package com.trendyol.mpc.kafkathena.commons.handler;

import com.trendyol.mpc.kafkathena.commons.model.KSConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface FailoverHandler {
    void handle(KSConsumer consumer, ConsumerRecord<?, ?> payload, Exception exception);
}