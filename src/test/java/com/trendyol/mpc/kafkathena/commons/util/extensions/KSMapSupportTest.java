package com.trendyol.mpc.kafkathena.commons.util.extensions;

import com.trendyol.mpc.kafkathena.commons.model.KSConsumerFactoryProp;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
class KSMapSupportTest {
    @Spy
    private KSMapSupport ksMapSupport;

    @Test
    public void it_should_MergeKafkaProps() {
        //given
        Map<String, Object> props = new HashMap<>();
        props.put("value.deserializer", "org.springframework.kafka.support.serializer.ErrorHandlingDeserializer");
        props.put("spring.deserializer.value.delegate.class", "org.springframework.kafka.support.serializer.JsonDeserializer");
        props.put("key.deserializer", "org.springframework.kafka.support.serializer.ErrorHandlingDeserializer");
        props.put("spring.deserializer.key.delegate.class", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("max.poll.records", 100);
        props.put("max.poll.interval.ms", 300000);
        props.put("session.timeout.ms", 300000);
        props.put("heartbeat.interval.ms", 3000);
        props.put("enable.auto.commit", true);
        props.put("auto.offset.reset", "earliest");
        props.put("fetch.max.bytes", 52428800);
        props.put("fetch.max.wait.ms", 500);

        Map<String, Object> defaultProps = new HashMap<>();
        defaultProps.put("value.deserializer", "demo.value.deserializer");
        defaultProps.put("spring.deserializer.value.delegate.class", "demo.deserializer.delegate");
        defaultProps.put("fetch.max.bytes", 52428800);
        defaultProps.put("fetch.max.wait.ms", 500);
        //when
        Map<String, Object> mergedProps = ksMapSupport.mergeKafkaProps(defaultProps, props);
        //then
        assertThat(mergedProps.get("value.deserializer")).isEqualTo("demo.value.deserializer");
        assertThat(mergedProps.get("spring.deserializer.value.delegate.class")).isEqualTo("demo.deserializer.delegate");
        assertThat(mergedProps.get("fetch.max.bytes")).isEqualTo(52428800);
        assertThat(mergedProps.get("fetch.max.wait.ms")).isEqualTo(500);
        assertThat(mergedProps.get("key.deserializer")).isEqualTo("org.springframework.kafka.support.serializer.ErrorHandlingDeserializer");
        assertThat(mergedProps.get("spring.deserializer.key.delegate.class")).isEqualTo("org.apache.kafka.common.serialization.StringDeserializer");
        assertThat(mergedProps.get("max.poll.records")).isEqualTo(100);
        assertThat(mergedProps.get("max.poll.interval.ms")).isEqualTo(300000);
        assertThat(mergedProps.get("session.timeout.ms")).isEqualTo(300000);
        assertThat(mergedProps.get("heartbeat.interval.ms")).isEqualTo(3000);
        assertThat(mergedProps.get("enable.auto.commit")).isEqualTo(true);
        assertThat(mergedProps.get("auto.offset.reset")).isEqualTo("earliest");

    }

    @Test
    public void it_should_MergeFactoryProps() {
        //given
        KSConsumerFactoryProp defaultFactoryProps = new KSConsumerFactoryProp();
        defaultFactoryProps.setAckMode(ContainerProperties.AckMode.RECORD);
        defaultFactoryProps.setConcurrency(1);
        defaultFactoryProps.setAutoStartup(true);
        defaultFactoryProps.setInterceptorClassPath("interceptor");
        defaultFactoryProps.setSyncCommit(true);
        defaultFactoryProps.setMissingTopicAlertEnable(false);
        defaultFactoryProps.setSyncCommitTimeoutSecond(40000);

        KSConsumerFactoryProp factoryProps = new KSConsumerFactoryProp();
        factoryProps.setAckMode(ContainerProperties.AckMode.BATCH);
        factoryProps.setConcurrency(5);
        factoryProps.setAutoStartup(false);
        factoryProps.setInterceptorClassPath("interceptor");
        ;

        //when
        KSConsumerFactoryProp mergedFactoryProps = ksMapSupport.mergeFactoryProps(defaultFactoryProps, factoryProps);
        //then
        assertThat(mergedFactoryProps.getAckMode()).isEqualTo(ContainerProperties.AckMode.BATCH);
        assertThat(mergedFactoryProps.getConcurrency()).isEqualTo(5);
        assertThat(mergedFactoryProps.getAutoStartup()).isEqualTo(false);
        assertThat(mergedFactoryProps.getInterceptorClassPath()).isEqualTo("interceptor");
        assertThat(defaultFactoryProps.getSyncCommit()).isEqualTo(true);
        assertThat(defaultFactoryProps.getMissingTopicAlertEnable()).isEqualTo(false);
        assertThat(defaultFactoryProps.getSyncCommitTimeoutSecond()).isEqualTo(40000);
    }

    @Test
    public void it_should_return_defaultProps_when_target_props_not_given() {
        //given
        KSConsumerFactoryProp defaultFactoryProps = new KSConsumerFactoryProp();
        defaultFactoryProps.setAckMode(ContainerProperties.AckMode.RECORD);
        defaultFactoryProps.setConcurrency(1);
        defaultFactoryProps.setAutoStartup(true);
        defaultFactoryProps.setInterceptorClassPath("interceptor");
        defaultFactoryProps.setSyncCommit(true);
        defaultFactoryProps.setMissingTopicAlertEnable(false);
        defaultFactoryProps.setSyncCommitTimeoutSecond(40000);

        //when
        KSConsumerFactoryProp mergedFactoryProps = ksMapSupport.mergeFactoryProps(defaultFactoryProps, null);
        //then
        assertThat(mergedFactoryProps.getAckMode()).isEqualTo(ContainerProperties.AckMode.RECORD);
        assertThat(mergedFactoryProps.getConcurrency()).isEqualTo(1);
        assertThat(mergedFactoryProps.getAutoStartup()).isEqualTo(true);
        assertThat(mergedFactoryProps.getInterceptorClassPath()).isEqualTo("interceptor");
        assertThat(defaultFactoryProps.getSyncCommit()).isEqualTo(true);
        assertThat(defaultFactoryProps.getMissingTopicAlertEnable()).isEqualTo(false);
        assertThat(defaultFactoryProps.getSyncCommitTimeoutSecond()).isEqualTo(40000);
    }
}