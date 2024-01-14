package com.trendyol.mpc.kafkathena.commons.util.extensions;

import com.trendyol.mpc.kafkathena.commons.model.KSConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface FailoverHandler {
    void handle(KSConsumer consumer, ConsumerRecord<?,?> record, Exception exception);
}