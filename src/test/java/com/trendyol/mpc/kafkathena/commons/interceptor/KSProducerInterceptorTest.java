package com.trendyol.mpc.kafkathena.commons.interceptor;

import ch.qos.logback.classic.Level;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(OutputCaptureExtension.class)
class KSProducerInterceptorTest {
    Logger logger = LoggerFactory.getLogger("com.trendyol.mpc.kafkathena.commons.interceptor.KSProducerInterceptor");
    @Test
    void onSend() {
        try (KSProducerInterceptor mock = new KSProducerInterceptor()) {
            ProducerRecord<String, Object> payload = new ProducerRecord<>("aa", null, null);
            mock.onSend(payload);
            assertThat(Arrays.stream(payload.headers().toArray()).map(Header::key).toList()).contains(KSProducerInterceptor.X_CORRELATION_ID);
        }
    }

    @Test
    void onAcknowledgement(CapturedOutput out) {
        ((ch.qos.logback.classic.Logger) logger).setLevel(Level.DEBUG);

        // add the appender to the logger
        try (KSProducerInterceptor mock = new KSProducerInterceptor()) {
            mock.onAcknowledgement(null, null);
            assertThat(out.getOut()).contains("KSProducerInterceptor onAcknowledgement");
        }


    }

    @Test
    void close(CapturedOutput out) {
        ((ch.qos.logback.classic.Logger) logger).setLevel(Level.DEBUG);

        // add the appender to the logger
        try (KSProducerInterceptor mock = new KSProducerInterceptor()) {
            mock.close();
            assertThat(out.getOut()).contains("KSProducerInterceptor close");
        }
    }

    @Test
    void configure(CapturedOutput out) {
        ((ch.qos.logback.classic.Logger) logger).setLevel(Level.DEBUG);

        // add the appender to the logger
        try (KSProducerInterceptor mock = new KSProducerInterceptor()) {
            mock.configure(null);
            assertThat(out.getOut()).contains("KSProducerInterceptor configure");
        }
    }
}