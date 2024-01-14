package com.trendyol.mpc.kafkathena.commons.util;

import com.trendyol.mpc.kafkathena.commons.model.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class KSConsumerHelperTest {

    @InjectMocks
    KSConsumerHelper ksConsumerHelper;

    @Mock
    private KSConsumerProducerProp consumerProps;

    @Mock
    private KSKafkaUtil ksKafkaUtil;

    @Test
    public void it_should_Init() {
        //given
        Map<String, KSConsumer> consumers = new HashMap<>();
        KSConsumer consumer1 = new KSConsumer();
        consumer1.setTopic("topic1");
        consumer1.setDataClass("blah.Message");
        consumer1.setFactoryBeanName("topic1FactoryBeanName");
        consumer1.setErrorProducerName("default");

        KSConsumerFailover topic1Failover = new KSConsumerFailover();
        topic1Failover.setErrorTopic("topic1.error");
        topic1Failover.setHandlerBeanName("topic1FailoverHandlerBeanName");
        consumer1.setFailover(topic1Failover);

        KSFixedRetry fixedRetry = new KSFixedRetry();
        fixedRetry.setRetryCount(1);
        fixedRetry.setBackoffIntervalMillis(4000L);
        consumer1.setFixedRetry(fixedRetry);

        Map<String, Object> consumer1Props = new HashMap<>();
        consumer1Props.put("value.deserializer", "org.springframework.kafka.support.serializer.ErrorHandlingDeserializer");
        consumer1Props.put("spring.deserializer.value.delegate.class", "org.springframework.kafka.support.serializer.JsonDeserializer");
        consumer1Props.put("key.deserializer", "org.springframework.kafka.support.serializer.ErrorHandlingDeserializer");
        consumer1Props.put("spring.deserializer.key.delegate.class", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer1Props.put("max.poll.records", 100);
        consumer1Props.put("max.poll.interval.ms", 300000);
        consumer1Props.put("session.timeout.ms", 300000);
        consumer1Props.put("heartbeat.interval.ms", 3000);
        consumer1Props.put("enable.auto.commit", true);
        consumer1Props.put("auto.offset.reset", "earliest");
        consumer1Props.put("fetch.max.bytes", 52428800);
        consumer1Props.put("fetch.max.wait.ms", 500);
        consumer1.setProps(consumer1Props);

        consumers.put("topic-1-consumer", consumer1);

        given(consumerProps.getConsumers()).willReturn(consumers);


        //when
        ksConsumerHelper.initConsumers();
        //then
        verify(ksKafkaUtil).createAndPublishListenerFactory(consumers.get("topic-1-consumer"));
    }

    public Map<String, KSProducer> getProducerMap() {
        Map<String, KSProducer> producers = new HashMap<>();
        KSProducer producer1 = new KSProducer();
        Map<String, Object> producer1ProducerProps = new HashMap<>();
        producer1ProducerProps.put("retries", 10);
        producer1ProducerProps.put("batch.size", 5242880);
        producer1ProducerProps.put("linger.ms", 100);
        producer1ProducerProps.put("buffer.memory", 33554432);
        producer1ProducerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer1ProducerProps.put("value.serializer", "org.springframework.kafka.support.serializer.JsonSerializer");
        producer1ProducerProps.put("acks", "1");
        producer1ProducerProps.put("request.timeout.ms", 30000);
        producer1ProducerProps.put("delivery.timeout.ms", 300500);

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
        producer1.setProps(producerProps);

        producers.put("default", producer1);

        return producers;
    }


}