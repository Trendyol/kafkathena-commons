package com.trendyol.mpc.kafkathena.commons.util;

import com.trendyol.mpc.kafkathena.commons.model.*;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class KSKafkaUtilTest {
    @InjectMocks
    KSKafkaUtil ksKafkaUtil;

    @Mock
    private KSKafkaHelper ksKafkaHandler;
    @Mock
    private ConfigurableApplicationContext applicationContext;
    @Mock
    private KSConsumerProducerProp consumerProps;

    @SneakyThrows
    @Test
    public void it_should_CreateSpringManagedFactory() {
        //given
        KSConsumer consumer1 = new KSConsumer();
        consumer1.setTopic("topic1");
        consumer1.setDataClass("com.trendyol.mpc.kafkathena.commons.model.DemoModel");
        consumer1.setFactoryBeanName("topic1FactoryBeanName");

        KSConsumerFactoryProp factoryProp = new KSConsumerFactoryProp();
        consumer1.setFactoryProps(factoryProp);

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
        consumer1Props.put("group.id", "group");
        consumer1.setProps(consumer1Props);

        given(consumerProps.getSharedFactoryProps()).willReturn(consumer1.getFactoryProps());

        ConsumerFactory factory = Mockito.mock(ConsumerFactory.class);
        given(ksKafkaHandler.createSpringConsumerFactory(eq(consumer1), eq(Class.forName(consumer1.getDataClass())), eq(consumer1.getFactoryProps()))).willReturn(factory);

        ConcurrentKafkaListenerContainerFactory listenerFactory = Mockito.mock(ConcurrentKafkaListenerContainerFactory.class);
        given(ksKafkaHandler.createSpringKafkaListenerContainerFactory(eq(factory), eq(consumer1), eq(consumer1.getFactoryProps()))).willReturn(listenerFactory);

        ConfigurableListableBeanFactory context = Mockito.mock(DefaultListableBeanFactory.class);
        given((applicationContext).getBeanFactory()).willReturn(context);
        ArgumentCaptor<ConcurrentKafkaListenerContainerFactory<String, ?>> concurrentKafkaListenerContainerFactoryArgumentCaptor = ArgumentCaptor.forClass(ConcurrentKafkaListenerContainerFactory.class);

        //when
        ksKafkaUtil.createAndPublishListenerFactory(consumer1);
        //then
        verify(context).registerSingleton(eq("topic1FactoryBeanName"), concurrentKafkaListenerContainerFactoryArgumentCaptor.capture());
    }
}