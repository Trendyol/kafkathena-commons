package com.trendyol.mpc.kafkathena.commons.util;

import com.trendyol.mpc.kafkathena.commons.model.*;
import com.trendyol.mpc.kafkathena.commons.model.constant.KSType;
import com.trendyol.mpc.kafkathena.commons.util.extensions.FailoverHandler;
import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class KSKafkaHelperTest {
    @InjectMocks
    private KSKafkaHelper ksKafkaHelper;

    @Mock
    private KafkaOperations<String, Object> defaultKafkaOperations;
    @Mock
    private KSConsumerProducerProp consumerProps;

    @Mock
    ConfigurableApplicationContext applicationContext;

    @Mock
    KSSenderDelegate ksSenderDelegate;

    @SneakyThrows
    @Test
    public void it_should_CreateSpringConsumerFactory() {
        //given
        KSConsumer ksConsumer = getKsConsumer(false, true, false, true, true, KSType.JSON);
        given(consumerProps.getConsumerPropsDefaults()).willReturn(ksConsumer.getProps());
        //when
        ConsumerFactory<String, ?> springConsumerFactory = ksKafkaHelper.createSpringConsumerFactory(ksConsumer, Class.forName(ksConsumer.getDataClass()), ksConsumer.getFactoryProps());
        //then
        assertThat(springConsumerFactory.getConfigurationProperties().get("max.poll.records")).isEqualTo(100);
    }

    @SneakyThrows
    @Test
    public void it_should_CreateSpringConsumerFactory_as_Avro() {
        //given
        KSConsumer ksConsumer = getKsConsumer(false, true, false, true, true, KSType.AVRO);
        given(consumerProps.getConsumerPropsDefaults()).willReturn(ksConsumer.getProps());
        //when
        ConsumerFactory<String, ?> springConsumerFactory = ksKafkaHelper.createSpringConsumerFactory(ksConsumer, Class.forName(ksConsumer.getDataClass()), ksConsumer.getFactoryProps());

        //then
        assertThat(springConsumerFactory.getConfigurationProperties().get("max.poll.records")).isEqualTo(100);
        assertThat(springConsumerFactory.getConfigurationProperties().get("spring.deserializer.value.delegate.class"))
                .isEqualTo("io.confluent.kafka.serializers.KafkaAvroDeserializer");
    }

    @SneakyThrows
    @Test
    public void it_should_CreateSpringConsumerFactory_as_Proto() {
        //given
        KSConsumer ksConsumer = getKsConsumer(false, true, false, true, true, KSType.PROTOBUF);
        given(consumerProps.getConsumerPropsDefaults()).willReturn(ksConsumer.getProps());
        //when
        ConsumerFactory<String, ?> springConsumerFactory = ksKafkaHelper.createSpringConsumerFactory(ksConsumer, Class.forName(ksConsumer.getDataClass()), ksConsumer.getFactoryProps());

        //then
        assertThat(springConsumerFactory.getConfigurationProperties().get("max.poll.records")).isEqualTo(100);
        assertThat(springConsumerFactory.getConfigurationProperties().get("spring.deserializer.value.delegate.class"))
                .isEqualTo("io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer");
    }

    @SneakyThrows
    @Test
    public void it_should_CreateSpringKafkaListenerContainerFactory() {
        //given
        KSConsumer ksConsumer = getKsConsumer(false, true, false, true, true, KSType.JSON);
        ksConsumer.setErrorProducerName("default");
        given(consumerProps.getConsumerPropsDefaults()).willReturn(ksConsumer.getProps());
        Map<String, KSProducer> producers = getProducerMap();
        given(ksSenderDelegate.checkProducer(ksConsumer.getErrorProducerName())).willReturn(true);
        //when
        ConsumerFactory<String, ?> springConsumerFactory = ksKafkaHelper.createSpringConsumerFactory(ksConsumer, Class.forName(ksConsumer.getDataClass()), ksConsumer.getFactoryProps());
        //when
        ConcurrentKafkaListenerContainerFactory<String, ?> springKafkaListenerContainerFactory = ksKafkaHelper.createSpringKafkaListenerContainerFactory(springConsumerFactory, ksConsumer, ksConsumer.getFactoryProps());
        //then
        assertThat(springKafkaListenerContainerFactory.getConsumerFactory()).isEqualTo(springConsumerFactory);
    }

    @SneakyThrows
    @Test
    public void it_should_createSpringKafkaListenerContainerFactory_with_exponential_retry() {
        //given
        KSConsumer ksConsumer = getKsConsumer(false, false, true, true, true, KSType.JSON);
        ksConsumer.setErrorProducerName("default");

        given(consumerProps.getConsumerPropsDefaults()).willReturn(ksConsumer.getProps());

        given(ksSenderDelegate.checkProducer(ksConsumer.getErrorProducerName())).willReturn(true);

        //when
        ConsumerFactory<String, ?> springConsumerFactory = ksKafkaHelper.createSpringConsumerFactory(ksConsumer, Class.forName(ksConsumer.getDataClass()), ksConsumer.getFactoryProps());
        //when
        ConcurrentKafkaListenerContainerFactory<String, ?> springKafkaListenerContainerFactory = ksKafkaHelper.createSpringKafkaListenerContainerFactory(springConsumerFactory, ksConsumer, ksConsumer.getFactoryProps());
        //then
        assertThat(springKafkaListenerContainerFactory.getConsumerFactory()).isEqualTo(springConsumerFactory);
    }


    @SneakyThrows
    @Test
    public void it_should_throw_exception_when_fixed_and_exponential_retry_not_defined() {
        //given
        KSConsumer ksConsumer = getKsConsumer(false, true, true, true, true, KSType.JSON);
        given(consumerProps.getConsumerPropsDefaults()).willReturn(ksConsumer.getProps());
        //when
        ConsumerFactory<String, ?> springConsumerFactory = ksKafkaHelper.createSpringConsumerFactory(ksConsumer, Class.forName(ksConsumer.getDataClass()), ksConsumer.getFactoryProps());
        //when
        Throwable throwable = catchThrowable(() -> ksKafkaHelper.createSpringKafkaListenerContainerFactory(springConsumerFactory, ksConsumer, ksConsumer.getFactoryProps()));
        //then
        assertThat(throwable).isInstanceOf(RuntimeException.class);
    }


    @SneakyThrows
    @Test
    public void it_should_getFailoverRecoverer_with_custom() {
        //given
        KSConsumer consumer = getKsConsumer(false, true, false, false, true, KSType.JSON);
        ConsumerRecord record = new ConsumerRecord("topic", -1, -1, null, null);
        Exception exception = new RuntimeException("");
        FailoverHandler failoverHandler = Mockito.mock(FailoverHandler.class);
        given(applicationContext.getBean(consumer.getFailover().getHandlerBeanName(), FailoverHandler.class)).willReturn(failoverHandler);

        //when
        ksKafkaHelper.getFailoverRecoverer(consumer)
                .accept(record, exception);
        //then
        verify(failoverHandler).handle(consumer, record, exception);
        verify(defaultKafkaOperations, never()).send(any(), any());
    }

    @SneakyThrows
    @Test
    public void it_should_getFailoverRecoverer_with_only_send_kafka_error() {
        //given
        KSConsumer consumer = getKsConsumer(false, true, false, true, false, KSType.JSON);
        consumer.setErrorProducerName("error-producer");
        ConsumerRecord record = new ConsumerRecord("topic", -1, -1, 123, "asdasd");
        Exception exception = new RuntimeException("");

        //when
        ksKafkaHelper.getFailoverRecoverer(consumer)
                .accept(record, exception);
        //then
        verifyNoInteractions(applicationContext);
        verify(ksSenderDelegate).send(eq(consumer.getErrorProducerName()),any(ProducerRecord.class));

    }

    enum ConsumerType {
        JSON, AVRO, PROTO
    }

    private KSConsumer getKsConsumer(boolean noRetry, boolean isFixedRetry, boolean isExponentialRetry, boolean isErrorTopic, boolean isCustomFailver, KSType consumerType) {
        KSConsumer consumer1 = new KSConsumer();
        consumer1.setTopic("topic1");
        consumer1.setDataClass("com.trendyol.mpc.kafkathena.commons.model.DemoModel");
        consumer1.setFactoryBeanName("topic1FactoryBeanName");
        consumer1.setType(consumerType.name());

        KSConsumerFactoryProp factoryProp = new KSConsumerFactoryProp();
        consumer1.setFactoryProps(factoryProp);

        KSConsumerFailover topic1Failover = new KSConsumerFailover();
        if (isErrorTopic) {
            topic1Failover.setErrorTopic("topic1.error");
        }
        if (isCustomFailver) {
            topic1Failover.setHandlerBeanName("topic1FailoverHandlerBeanName");
        }
        consumer1.setFailover(topic1Failover);

        if (!noRetry && isFixedRetry) {
            KSFixedRetry fixedRetry = new KSFixedRetry();
            fixedRetry.setRetryCount(1);
            fixedRetry.setBackoffIntervalMillis(4000L);
            consumer1.setFixedRetry(fixedRetry);
        }

        if (!noRetry && isExponentialRetry) {
            KSExponentialRetry exponentialRetry = new KSExponentialRetry();
            exponentialRetry.setRetryCount(1);
            exponentialRetry.setMultiplier(1.0);
            exponentialRetry.setBackoffIntervalMillis(1000L);
            exponentialRetry.setMaxInterval(100000L);
            consumer1.setExponentialRetry(exponentialRetry);
        }

        Map<String, Object> consumer1Props = new HashMap<>();
        consumer1Props.put("value.deserializer", "org.springframework.kafka.support.serializer.ErrorHandlingDeserializer");
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
        if (consumerType == KSType.AVRO) {
            consumer1Props.put("schema.registry.url", "http://schema-registry");
            consumer1Props.put("specific.avro.reader", true);
            consumer1Props.put("spring.deserializer.value.delegate.class", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        } else if (consumerType == KSType.PROTOBUF) {
            consumer1Props.put("schema.registry.url", "http://schema-registry");
            consumer1Props.put("specific.protobuf.value.type", "proto.Person");
            consumer1Props.put("spring.deserializer.value.delegate.class", "io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer");
        }
        consumer1.setProps(consumer1Props);
        return consumer1;
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