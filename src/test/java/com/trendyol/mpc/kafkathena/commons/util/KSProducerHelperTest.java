package com.trendyol.mpc.kafkathena.commons.util;

import com.trendyol.mpc.kafkathena.commons.model.KSConsumerProducerProp;
import com.trendyol.mpc.kafkathena.commons.model.KSProducer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;

@ExtendWith(MockitoExtension.class)
class KSProducerHelperTest {

    @InjectMocks
    private KSProducerHelper ksProducerHelper;

    @Mock
    private KSConsumerProducerProp ksConsumerProducerProps;

    @Test
    public void it_should_init() {
        //given
        ConcurrentHashMap<String, KSProducer> producers = new ConcurrentHashMap<>();
        KSProducer producer1 = new KSProducer();
        Map<String, Object> props = new HashMap<>();
        props.put("retries", 10);
        props.put("batch.size", 5242880);
        props.put("linger.ms", 100);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.springframework.kafka.support.serializer.JsonSerializer");
        props.put("acks", "1");
        props.put("request.timeout.ms", 30000);
        props.put("delivery.timeout.ms", 300500);

        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put("retries", 10);
        producerProps.put("batch.size", 5242880);
        producerProps.put("linger.ms", 100);
        producerProps.put("buffer.memory", 33554432);
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.springframework.kafka.support.serializer.JsonSerializer");
        producerProps.put("acks", "1");
        producerProps.put("request.timeout.ms", 30000);
        producerProps.put("delivery.timeout.ms", 300600);


        producer1.setProps(props);
        producers.put("producer-1", producer1);

        given(ksConsumerProducerProps.getProducers()).willReturn(producers);
        given(ksConsumerProducerProps.getProducerPropsDefaults()).willReturn(props);

        //when
        ksProducerHelper.initProducers();
        //then
        KafkaTemplate<String, Object> producer = ksProducerHelper.getProducerMap().get("producer-1_kt_producer");
        assertThat(producer.getProducerFactory().getConfigurationProperties().get("retries")).isEqualTo(10);
        assertThat(producer.getProducerFactory().getConfigurationProperties().get("batch.size")).isEqualTo(5242880);
        assertThat(producer.getProducerFactory().getConfigurationProperties().get("linger.ms")).isEqualTo(100);
        assertThat(producer.getProducerFactory().getConfigurationProperties().get("buffer.memory")).isEqualTo(33554432);
        assertThat(producer.getProducerFactory().getConfigurationProperties().get("key.serializer")).isEqualTo("org.apache.kafka.common.serialization.StringSerializer");
        assertThat(producer.getProducerFactory().getConfigurationProperties().get("value.serializer")).isEqualTo("org.springframework.kafka.support.serializer.JsonSerializer");
        assertThat(producer.getProducerFactory().getConfigurationProperties().get("acks")).isEqualTo("1");
        assertThat(producer.getProducerFactory().getConfigurationProperties().get("request.timeout.ms")).isEqualTo(30000);
        assertThat(producer.getProducerFactory().getConfigurationProperties().get("delivery.timeout.ms")).isEqualTo(300500);
    }

}