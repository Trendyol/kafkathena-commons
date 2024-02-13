package com.trendyol.mpc.kafkathena.commons.util.extensions;

import com.trendyol.mpc.kafkathena.commons.model.sharedfactory.KSSharedConsumerFactoryProperties;
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
    void it_should_MergeKafkaProps() {
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

        Map<String, Object> checkMap = new HashMap<>();
        checkMap.put("spring.deserializer.value.delegate.class", "demo.deserializer.delegate");
        checkMap.put("fetch.max.bytes", 52428800);
        checkMap.put("fetch.max.wait.ms", 500);
        checkMap.put("key.deserializer", "org.springframework.kafka.support.serializer.ErrorHandlingDeserializer");
        checkMap.put("spring.deserializer.key.delegate.class", "org.apache.kafka.common.serialization.StringDeserializer");
        checkMap.put("max.poll.records", 100);
        checkMap.put("max.poll.interval.ms", 300000);
        checkMap.put("session.timeout.ms", 300000);
        checkMap.put("heartbeat.interval.ms", 3000);
        checkMap.put("enable.auto.commit", true);
        checkMap.put("auto.offset.reset", "earliest");

        assertThat(mergedProps).containsAllEntriesOf(checkMap);


    }

    @Test
    void it_should_MergeFactoryProps() {
        //given
        KSSharedConsumerFactoryProperties defaultFactoryProps = new KSSharedConsumerFactoryProperties();
        defaultFactoryProps.setAckMode(ContainerProperties.AckMode.RECORD);
        defaultFactoryProps.setConcurrency(1);
        defaultFactoryProps.setAutoStartup(true);
        defaultFactoryProps.setInterceptor("interceptor");
        defaultFactoryProps.setSyncCommit(true);
        defaultFactoryProps.setMissingTopicAlertEnable(false);
        defaultFactoryProps.setSyncCommitTimeoutSecond(40000);

        KSSharedConsumerFactoryProperties factoryProps = new KSSharedConsumerFactoryProperties();
        factoryProps.setAckMode(ContainerProperties.AckMode.BATCH);
        factoryProps.setConcurrency(5);
        factoryProps.setAutoStartup(false);
        factoryProps.setInterceptor("interceptor");
        ;

        //when
        KSSharedConsumerFactoryProperties mergedFactoryProps = ksMapSupport.mergeConsumerFactoryProps(defaultFactoryProps, factoryProps);
        //then
        assertThat(mergedFactoryProps.getAckMode()).isEqualTo(ContainerProperties.AckMode.BATCH);
        assertThat(mergedFactoryProps.getConcurrency()).isEqualTo(5);
        assertThat(mergedFactoryProps.getAutoStartup()).isFalse();
        assertThat(mergedFactoryProps.getInterceptor()).isEqualTo("interceptor");
        assertThat(defaultFactoryProps.getSyncCommit()).isTrue();
        assertThat(defaultFactoryProps.getMissingTopicAlertEnable()).isFalse();
        assertThat(defaultFactoryProps.getSyncCommitTimeoutSecond()).isEqualTo(40000);
    }

    @Test
    void it_should_return_defaultProps_when_target_props_not_given() {
        //given
        KSSharedConsumerFactoryProperties defaultFactoryProps = new KSSharedConsumerFactoryProperties();
        defaultFactoryProps.setAckMode(ContainerProperties.AckMode.RECORD);
        defaultFactoryProps.setConcurrency(1);
        defaultFactoryProps.setAutoStartup(true);
        defaultFactoryProps.setInterceptor("interceptor");
        defaultFactoryProps.setSyncCommit(true);
        defaultFactoryProps.setMissingTopicAlertEnable(false);
        defaultFactoryProps.setSyncCommitTimeoutSecond(40000);

        //when
        KSSharedConsumerFactoryProperties mergedFactoryProps = ksMapSupport.mergeConsumerFactoryProps(defaultFactoryProps, null);
        //then
        assertThat(mergedFactoryProps.getAckMode()).isEqualTo(ContainerProperties.AckMode.RECORD);
        assertThat(mergedFactoryProps.getConcurrency()).isEqualTo(1);
        assertThat(mergedFactoryProps.getAutoStartup()).isTrue();
        assertThat(mergedFactoryProps.getInterceptor()).isEqualTo("interceptor");
        assertThat(defaultFactoryProps.getSyncCommit()).isTrue();
        assertThat(defaultFactoryProps.getMissingTopicAlertEnable()).isFalse();
        assertThat(defaultFactoryProps.getSyncCommitTimeoutSecond()).isEqualTo(40000);
    }
}