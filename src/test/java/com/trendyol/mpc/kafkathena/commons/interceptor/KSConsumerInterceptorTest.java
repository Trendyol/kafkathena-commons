package com.trendyol.mpc.kafkathena.commons.interceptor;

import ch.qos.logback.classic.Level;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(OutputCaptureExtension.class)
class KSConsumerInterceptorTest {
    Logger logger = LoggerFactory.getLogger("com.trendyol.mpc.kafkathena.commons.interceptor.KSConsumerInterceptor");
    @Test
    void onConsume() {
        try (KSConsumerInterceptor mock = new KSConsumerInterceptor()) {
            Map<TopicPartition, List<ConsumerRecord<String, String>>> records = new HashMap<>();
            records.put(new TopicPartition("topic", 1), List.of(new ConsumerRecord<String, String>("topic", 1, 1, null, null)));
            ConsumerRecords<String, String> consumerRecords = new ConsumerRecords<>(records);
            mock.onConsume(consumerRecords);
            assertThat(Arrays.stream(consumerRecords.iterator().next().headers().toArray()).map(Header::key).toList()).contains(KSConsumerInterceptor.X_CORRELATION_ID);
        }
    }

    @Test
    void onConsume_with_existing_correlation_id() {
        try (KSConsumerInterceptor mock = new KSConsumerInterceptor()) {
            Map<TopicPartition, List<ConsumerRecord<String, String>>> records = new HashMap<>();
            ConsumerRecord<String, String> consumerRecord = new ConsumerRecord<>("topic", 1, 1, null, null);
            consumerRecord.headers().add(KSConsumerInterceptor.X_CORRELATION_ID, "correlation-id".getBytes(StandardCharsets.UTF_8));
            records.put(new TopicPartition("topic", 1), List.of(consumerRecord));
            ConsumerRecords<String, String> consumerRecords = new ConsumerRecords<>(records);
            mock.onConsume(consumerRecords);
            assertThat(Arrays.stream(consumerRecords.iterator().next().headers().toArray()).map(Header::key).toList()).contains(KSConsumerInterceptor.X_CORRELATION_ID);
        }
    }

    @Test
    void onCommit(CapturedOutput out) {
        ((ch.qos.logback.classic.Logger) logger).setLevel(Level.DEBUG);
        // add the appender to the logger
        try (KSConsumerInterceptor mock = new KSConsumerInterceptor()) {
            mock.onCommit(null);
            assertThat(out.getOut()).contains("KSConsumerInterceptor onCommit");
        }
    }

    @Test
    void close(CapturedOutput out) {
        ((ch.qos.logback.classic.Logger) logger).setLevel(Level.DEBUG);

        // add the appender to the logger
        try (KSConsumerInterceptor mock = new KSConsumerInterceptor()) {
            mock.close();
            assertThat(out.getOut()).contains("KSConsumerInterceptor close");
        }
    }

    @Test
    void configure(CapturedOutput out) {
        ((ch.qos.logback.classic.Logger) logger).setLevel(Level.DEBUG);

        // add the appender to the logger
        try (KSConsumerInterceptor mock = new KSConsumerInterceptor()) {
            mock.configure(null);
            assertThat(out.getOut()).contains("KSConsumerInterceptor configure");
        }
    }
}