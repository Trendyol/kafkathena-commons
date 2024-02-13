package com.trendyol.mpc.kafkathena.commons.util;

import com.trendyol.mpc.kafkathena.commons.handler.KSKafkaHelper;
import com.trendyol.mpc.kafkathena.commons.handler.KSSenderDelegate;
import com.trendyol.mpc.kafkathena.commons.handler.KafkathenaConfigurationHelper;
import com.trendyol.mpc.kafkathena.commons.model.*;
import com.trendyol.mpc.kafkathena.commons.model.constant.KSType;
import com.trendyol.mpc.kafkathena.commons.model.exception.KSException;
import com.trendyol.mpc.kafkathena.commons.handler.FailoverHandler;
import com.trendyol.mpc.kafkathena.commons.model.sharedfactory.KSSharedConsumerFactoryProperties;
import com.trendyol.mpc.kafkathena.commons.model.sharedfactory.KSSharedFactoryProperties;
import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.BeanInstantiationException;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaOperations;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class KSKafkaHelperTest {
    @InjectMocks
    KSKafkaHelper ksKafkaHelper;
    @Mock
    ConfigurableApplicationContext applicationContext;
    @Mock
    KSSenderDelegate ksSenderDelegate;
    @Mock
    KafkaOperations<String, Object> defaultKafkaOperations;
    @Mock
    KSConfigurationProperties consumerProps;
    KSConfigurationProperties configurationProperties;
    KSConsumer ksConsumer;
    KSProducer ksProducer;

    @BeforeEach
    public void setUp() {
        configurationProperties = KafkathenaConfigurationHelper.getConfigurationProperties();
        KSSharedFactoryProperties sharedFactoryProps = KafkathenaConfigurationHelper.createSharedFactoryProps();
        Map<String, Object> consumerSharedDefaultProps = KafkathenaConfigurationHelper.createConsumerSharedDefaultProps();
        Map<String, Object> producerSharedDefaultProps = KafkathenaConfigurationHelper.createProducerSharedDefaults();
        ksConsumer = KafkathenaConfigurationHelper.addConsumerToConfiguration(configurationProperties, "consumer", "default", false, true, true, KSType.JSON, KafkathenaConfigurationHelper.RetryType.FIXED);
        ksProducer = KafkathenaConfigurationHelper.addProducerToConfiguration(configurationProperties, "default", "default", false);
        configurationProperties.setSharedFactoryProps(sharedFactoryProps);
        configurationProperties.setConsumerPropsDefaults(consumerSharedDefaultProps);
        configurationProperties.setProducerPropsDefaults(producerSharedDefaultProps);
    }

    @SneakyThrows
    @Test
    void it_should_CreateSpringConsumerFactory() {
        //when
        ConsumerFactory<String, ?> springConsumerFactory = ksKafkaHelper.createSpringConsumerFactory(ksConsumer, Class.forName(ksConsumer.getDataClass()), ksConsumer.getProps(), ksConsumer.getFactoryProps());
        //then
        assertThat(springConsumerFactory).isNotNull();
        assertThat(springConsumerFactory.getConfigurationProperties()).usingRecursiveComparison().isEqualTo(ksConsumer.getProps());
    }

    @SneakyThrows
    @Test
    void it_should_CreateSpringConsumerFactory_as_Avro() {
        //given
        ksConsumer = KafkathenaConfigurationHelper.createConsumer("default", "default", true, true, true, KSType.AVRO, KafkathenaConfigurationHelper.RetryType.FIXED);

        //when
        ConsumerFactory<String, ?> springConsumerFactory = ksKafkaHelper.createSpringConsumerFactory(ksConsumer, Class.forName(ksConsumer.getDataClass()), ksConsumer.getProps(), ksConsumer.getFactoryProps());

        //then
        assertThat(springConsumerFactory).isNotNull();
        assertThat(springConsumerFactory.getConfigurationProperties()).usingRecursiveComparison().isEqualTo(ksConsumer.getProps());
    }

    @SneakyThrows
    @Test
    void it_should_CreateSpringConsumerFactory_as_Proto() {
        //given
        ksConsumer = KafkathenaConfigurationHelper.createConsumer("default", "default", true, true, true, KSType.PROTOBUF, KafkathenaConfigurationHelper.RetryType.FIXED);
        //when
        ConsumerFactory<String, ?> springConsumerFactory = ksKafkaHelper.createSpringConsumerFactory(ksConsumer, Class.forName(ksConsumer.getDataClass()), ksConsumer.getProps(), ksConsumer.getFactoryProps());

        //then
        assertThat(springConsumerFactory).isNotNull();
        assertThat(springConsumerFactory.getConfigurationProperties()).usingRecursiveComparison().isEqualTo(ksConsumer.getProps());
    }

    @SneakyThrows
    @Test
    void it_should_CreateSpringKafkaListenerContainerFactory() {
        //given
        ksConsumer = KafkathenaConfigurationHelper.createConsumer("default", "default", true, true, true, KSType.JSON, KafkathenaConfigurationHelper.RetryType.FIXED);

        ksConsumer.setErrorProducerName("default");
        given(ksSenderDelegate.checkProducer(ksConsumer.getErrorProducerName())).willReturn(true);
        //when
        ConsumerFactory<String, ?> springConsumerFactory = ksKafkaHelper.createSpringConsumerFactory(ksConsumer, Class.forName(ksConsumer.getDataClass()), ksConsumer.getProps(), ksConsumer.getFactoryProps());
        //when
        ConcurrentKafkaListenerContainerFactory<String, ?> springKafkaListenerContainerFactory = ksKafkaHelper.createSpringKafkaListenerContainerFactory(springConsumerFactory, ksConsumer, ksConsumer.getFactoryProps());
        //then
        assertThat(springKafkaListenerContainerFactory.getConsumerFactory()).isEqualTo(springConsumerFactory);
        assertThat(springKafkaListenerContainerFactory.getConsumerFactory().getConfigurationProperties()).usingRecursiveComparison()
                .isEqualTo(springConsumerFactory.getConfigurationProperties());
    }

    @SneakyThrows
    @Test
    void it_should_createSpringKafkaListenerContainerFactory_with_exponential_retry() {
        //given
        ksConsumer = KafkathenaConfigurationHelper.createConsumer("default", "default", true, true, true, KSType.JSON, KafkathenaConfigurationHelper.RetryType.FIXED);
        ksConsumer.setErrorProducerName("default");


        given(ksSenderDelegate.checkProducer(ksConsumer.getErrorProducerName())).willReturn(true);

        //when
        ConsumerFactory<String, ?> springConsumerFactory = ksKafkaHelper.createSpringConsumerFactory(ksConsumer, Class.forName(ksConsumer.getDataClass()), ksConsumer.getProps(), ksConsumer.getFactoryProps());
        //when
        ConcurrentKafkaListenerContainerFactory<String, ?> springKafkaListenerContainerFactory = ksKafkaHelper.createSpringKafkaListenerContainerFactory(springConsumerFactory, ksConsumer, ksConsumer.getFactoryProps());
        //then
        assertThat(springKafkaListenerContainerFactory.getConsumerFactory()).isEqualTo(springConsumerFactory);
        assertThat(springKafkaListenerContainerFactory.getConsumerFactory().getConfigurationProperties()).usingRecursiveComparison()
                .isEqualTo(springConsumerFactory.getConfigurationProperties());
    }

    @SneakyThrows
    @Test
    void it_should_throw_exception_when_fixed_and_exponential_retry_not_defined() {
        //given
        ksConsumer = KafkathenaConfigurationHelper.createConsumer("default", "default", true, true, true, KSType.JSON, KafkathenaConfigurationHelper.RetryType.FIXED);
        //when
        ConsumerFactory<String, ?> springConsumerFactory = ksKafkaHelper.createSpringConsumerFactory(ksConsumer, Class.forName(ksConsumer.getDataClass()), ksConsumer.getProps(), ksConsumer.getFactoryProps());
        //when
        Throwable throwable = catchThrowable(() -> ksKafkaHelper.createSpringKafkaListenerContainerFactory(springConsumerFactory, ksConsumer, ksConsumer.getFactoryProps()));
        //then
        assertThat(throwable).isInstanceOf(RuntimeException.class);
    }

    @SneakyThrows
    @Test
    void it_should_getFailoverRecoverer_with_custom() {
        //given
        ksConsumer = KafkathenaConfigurationHelper.createConsumer("default", "default", true, true, true, KSType.JSON, KafkathenaConfigurationHelper.RetryType.FIXED);
        ConsumerRecord record = new ConsumerRecord("topic", -1, -1, null, null);
        Exception exception = new RuntimeException("");
        FailoverHandler failoverHandler = Mockito.mock(FailoverHandler.class);
        given(applicationContext.getBean(ksConsumer.getFailover().getHandlerBeanName(), FailoverHandler.class)).willReturn(failoverHandler);

        //when
        ksKafkaHelper.getFailoverRecoverer(ksConsumer)
                .accept(record, exception);
        //then
        verify(failoverHandler).handle(ksConsumer, record, exception);
        verify(defaultKafkaOperations, never()).send(any(), any());
    }

    @SneakyThrows
    @Test
    void it_should_getFailoverRecoverer_with_only_send_kafka_error() {
        //given
        ksConsumer = KafkathenaConfigurationHelper.createConsumer("default", "default", true, true, false, KSType.JSON, KafkathenaConfigurationHelper.RetryType.FIXED);
        ksConsumer.setErrorProducerName("error-producer");
        ConsumerRecord record = new ConsumerRecord("topic", -1, -1, 123, "asdasd");
        Exception exception = new RuntimeException("");

        //when
        ksKafkaHelper.getFailoverRecoverer(ksConsumer)
                .accept(record, exception);
        //then
        verifyNoInteractions(applicationContext);
        verify(ksSenderDelegate).send(eq(ksConsumer.getErrorProducerName()), any(ProducerRecord.class));
    }

    @Test
    void it_should_return_true_when_exception_is_ignored(){
        Exception ex = new RuntimeException();
        KSConsumerFailover failover = new KSConsumerFailover();
        failover.setIgnoredExceptionClasses(List.of(ex.getClass().getName()));
        KSConsumer consumer = new KSConsumer();
        consumer.setFailover(failover);
        boolean ignoredException = ksKafkaHelper.isIgnoredException(consumer, ex);
        assertThat(ignoredException).isTrue();
    }

    @Test
    void it_should_return_false_when_exception_is_not_ignored(){
        Exception ex = new RuntimeException();
        KSConsumerFailover failover = new KSConsumerFailover();
        failover.setIgnoredExceptionClasses(List.of());
        KSConsumer consumer = new KSConsumer();
        consumer.setFailover(failover);
        boolean ignoredException = ksKafkaHelper.isIgnoredException(consumer, ex);
        assertThat(ignoredException).isFalse();
    }

    @Test
    void it_should_return_true_when_exception_cause_is_ignored(){
        IllegalArgumentException iae = new IllegalArgumentException();
        Exception ex = new RuntimeException(iae);
        KSConsumerFailover failover = new KSConsumerFailover();
        failover.setIgnoredExceptionClasses(List.of(ex.getClass().getName()));
        KSConsumer consumer = new KSConsumer();
        consumer.setFailover(failover);
        boolean ignoredException = ksKafkaHelper.isIgnoredException(consumer, ex);
        assertThat(ignoredException).isTrue();
    }

    @Test
    void it_should_return_false_when_exception_cause_is_not_ignored(){
        IllegalArgumentException iae = new IllegalArgumentException();
        Exception ex = new RuntimeException(iae);
        KSConsumerFailover failover = new KSConsumerFailover();
        failover.setIgnoredExceptionClasses(List.of());
        KSConsumer consumer = new KSConsumer();
        consumer.setFailover(failover);
        boolean ignoredException = ksKafkaHelper.isIgnoredException(consumer, ex);
        assertThat(ignoredException).isFalse();
    }

    @Test
    void it_should_apply_fixed_retry_mechanism(){
        KSConsumer ksConsumer1 = new KSConsumer();
        KSFixedRetry ksFixedRetry = new KSFixedRetry();
        ksFixedRetry.setRetryCount(1);
        ksFixedRetry.setBackoffIntervalMillis(1000L);
        ksConsumer1.setFixedRetry(ksFixedRetry);
        ConsumerFactory<String,Object> fa = new DefaultKafkaConsumerFactory<>(new HashMap<>());
        ConcurrentKafkaListenerContainerFactory<String, ?> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(fa);
        ksKafkaHelper.applyRetryMechanismToFactory(ksConsumer1, factory);
        assertThat(factory.getContainerProperties()).isNotNull();
    }

    @Test
    void it_should_apply_exponential_retry_mechanism(){
        KSConsumer ksConsumer1 = new KSConsumer();
        KSExponentialRetry retry = new KSExponentialRetry();
        retry.setRetryCount(1);
        retry.setMultiplier(1.0);
        retry.setMaxInterval(19L);
        retry.setBackoffIntervalMillis(100L);
        ksConsumer1.setExponentialRetry(retry);
        ConcurrentKafkaListenerContainerFactory<String, ?> factory = new ConcurrentKafkaListenerContainerFactory<>();
        ksKafkaHelper.applyRetryMechanismToFactory(ksConsumer1, factory);
        assertThat(factory.getContainerProperties()).isNotNull();
    }

    @Test
    void it_should_not_apply_retry_mechanism_when_both_exponential_and_fixed_are_defined(){
        KSConsumer ksConsumer1 = new KSConsumer();
        ksConsumer1.setFixedRetry(new KSFixedRetry());
        ksConsumer1.setExponentialRetry(new KSExponentialRetry());
        Throwable throwable = catchThrowable(() -> ksKafkaHelper.applyRetryMechanismToFactory(ksConsumer1, new ConcurrentKafkaListenerContainerFactory<>()));
        assertThat(throwable).isInstanceOf(KSException.class);
    }

    @Test
    void it_should_invoke_error_topic_action_with_filter_key_and_custom_header(){
        KSConsumer ksConsumer1 = new KSConsumer();
        KSConsumerFailover failover = new KSConsumerFailover();
        failover.setErrorTopic("error-topic");
        ksConsumer1.setFailover(failover);
        KSFilterHeader filter = new KSFilterHeader();
        filter.setErrorProducerFilterKey("error-filter");
        filter.setCustomHeaders(List.of(new KSFilterHeader.CustomKsFilterHeader("key", "value")));
        ksConsumer1.setFilterHeader(filter);

        KSException ex = new KSException("");
        ConsumerRecord<String, String> payload = new ConsumerRecord<>("topic", 1, 1, "key", "payload");

        KSKafkaHelper spy = spy(ksKafkaHelper);
        doReturn(List.of()).when(spy).getIgnoredExceptionClasses(ksConsumer1);
        ArgumentCaptor<ProducerRecord> producerRecordCaptor = ArgumentCaptor.forClass(ProducerRecord.class);

        spy.invokeErrorTopicAction(ksConsumer1, payload, ex);
        verify(ksSenderDelegate).send(eq(ksConsumer1.getErrorProducerName()), producerRecordCaptor.capture());
    }

    @Test
    void it_should_invoke_error_topic_action_with_custom_header(){
        KSConsumer ksConsumer1 = new KSConsumer();
        KSConsumerFailover failover = new KSConsumerFailover();
        failover.setErrorTopic("error-topic");
        ksConsumer1.setFailover(failover);
        KSFilterHeader filter = new KSFilterHeader();
        filter.setCustomHeaders(List.of(new KSFilterHeader.CustomKsFilterHeader("key", "value")));
        ksConsumer1.setFilterHeader(filter);

        KSException ex = new KSException("");
        ConsumerRecord<String, String> payload = new ConsumerRecord<>("topic", 1, 1, "key", "payload");

        KSKafkaHelper spy = spy(ksKafkaHelper);
        doReturn(List.of()).when(spy).getIgnoredExceptionClasses(ksConsumer1);
        ArgumentCaptor<ProducerRecord> producerRecordCaptor = ArgumentCaptor.forClass(ProducerRecord.class);

        spy.invokeErrorTopicAction(ksConsumer1, payload, ex);
        verify(ksSenderDelegate).send(eq(ksConsumer1.getErrorProducerName()), producerRecordCaptor.capture());
    }

    @Test
    void it_should_invoke_error_topic_action_with_no_filter(){
        KSConsumer ksConsumer1 = new KSConsumer();
        KSConsumerFailover failover = new KSConsumerFailover();
        failover.setErrorTopic("error-topic");
        ksConsumer1.setFailover(failover);

        KSException ex = new KSException("");
        ConsumerRecord<String, String> payload = new ConsumerRecord<>("topic", 1, 1, "key", "payload");

        KSKafkaHelper spy = spy(ksKafkaHelper);
        doReturn(List.of()).when(spy).getIgnoredExceptionClasses(ksConsumer1);
        ArgumentCaptor<ProducerRecord> producerRecordCaptor = ArgumentCaptor.forClass(ProducerRecord.class);

        spy.invokeErrorTopicAction(ksConsumer1, payload, ex);
        verify(ksSenderDelegate).send(eq(ksConsumer1.getErrorProducerName()), producerRecordCaptor.capture());
    }

    @Test
    void it_should_not_invoke_error_topic_action_with_ignored_exception(){
        KSConsumer ksConsumer1 = new KSConsumer();
        KSConsumerFailover failover = new KSConsumerFailover();
        failover.setErrorTopic("error-topic");
        ksConsumer1.setFailover(failover);

        KSException ex = new KSException("");
        ConsumerRecord<String, String> payload = new ConsumerRecord<>("topic", 1, 1, "key", "payload");

        KSKafkaHelper spy = spy(ksKafkaHelper);
        doReturn(List.of(ex.getClass())).when(spy).getIgnoredExceptionClasses(ksConsumer1);
        ArgumentCaptor<ProducerRecord> producerRecordCaptor = ArgumentCaptor.forClass(ProducerRecord.class);

        spy.invokeErrorTopicAction(ksConsumer1, payload, ex);
        verify(ksSenderDelegate, never()).send(eq(ksConsumer1.getErrorProducerName()), producerRecordCaptor.capture());
    }

    @Test
    void it_should_not_invoke_error_topic_action_when_send_has_an_exception(){
        //given
        KSConsumer ksConsumer1 = new KSConsumer();
        KSConsumerFailover failover = new KSConsumerFailover();
        failover.setErrorTopic("error-topic");
        ksConsumer1.setFailover(failover);

        KSException ex = new KSException("");
        ConsumerRecord<String, String> payload = new ConsumerRecord<>("topic", 1, 1, "key", "payload");

        KSKafkaHelper spy = spy(ksKafkaHelper);
        doThrow(RuntimeException.class).when(spy).isIgnoredException(ksConsumer1, ex);

        //when
        Throwable throwable = catchThrowable(() -> spy.invokeErrorTopicAction(ksConsumer1, payload, ex));

        //then
        verifyNoInteractions(ksSenderDelegate);
    }

    @Test
    void it_should_invoke_failover_handler_if_define(){
        KSConsumer ksConsumer1 = new KSConsumer();
        KSConsumerFailover failover = new KSConsumerFailover();
        failover.setHandlerBeanName("handler");
        ksConsumer1.setFailover(failover);

        KSException ex = new KSException("");
        ConsumerRecord<String, String> payload = new ConsumerRecord<>("topic", 1, 1, "key", "payload");
        FailoverHandler failoverHandler = Mockito.mock(FailoverHandler.class);
        KSKafkaHelper spy = spy(ksKafkaHelper);
        doReturn(List.of()).when(spy).getIgnoredExceptionClasses(ksConsumer1);
        given(applicationContext.getBean(ksConsumer1.getFailover().getHandlerBeanName(), FailoverHandler.class)).willReturn(failoverHandler);

        spy.invokeFailoverHandler(ksConsumer1, payload, ex);
        verify(failoverHandler).handle(ksConsumer1, payload, ex);
    }

    @Test
    void it_should_not_invoke_failover_handler_if_define(){
        KSConsumer ksConsumer1 = new KSConsumer();
        KSConsumerFailover failover = new KSConsumerFailover();
        failover.setHandlerBeanName("handler");
        ksConsumer1.setFailover(failover);

        KSException ex = new KSException("");
        ConsumerRecord<String, String> payload = new ConsumerRecord<>("topic", 1, 1, "key", "payload");
        FailoverHandler failoverHandler = Mockito.mock(FailoverHandler.class);
        KSKafkaHelper spy = spy(ksKafkaHelper);
        doReturn(true).when(spy).isIgnoredException(ksConsumer1, ex);

        spy.invokeFailoverHandler(ksConsumer1, payload, ex);
        verifyNoInteractions(applicationContext);
    }

    @Test
    void it_should_invoke_failover_handler_if_specify_but_not_define_as_bean(){
        KSConsumer ksConsumer1 = new KSConsumer();
        KSConsumerFailover failover = new KSConsumerFailover();
        failover.setHandlerBeanName("handler");
        ksConsumer1.setFailover(failover);

        KSException ex = new KSException("");
        ConsumerRecord<String, String> payload = new ConsumerRecord<>("topic", 1, 1, "key", "payload");
        FailoverHandler failoverHandler = Mockito.mock(FailoverHandler.class);
        KSKafkaHelper spy = spy(ksKafkaHelper);
        doReturn(List.of()).when(spy).getIgnoredExceptionClasses(ksConsumer1);

        doThrow(BeanInstantiationException.class).when(applicationContext).getBean(ksConsumer1.getFailover().getHandlerBeanName(), FailoverHandler.class);

        Throwable throwable = catchThrowable(() -> spy.invokeFailoverHandler(ksConsumer1, payload, ex));
        assertThat(throwable).isInstanceOf(KSException.class);
        verify(failoverHandler, never()).handle(ksConsumer1, payload, ex);
    }

    @Test
    void it_should_invoke_failover_handler_if_specify_but_not_define_as_bean_with_runtime_exception(){
        KSConsumer ksConsumer1 = new KSConsumer();
        KSConsumerFailover failover = new KSConsumerFailover();
        failover.setHandlerBeanName("handler");
        ksConsumer1.setFailover(failover);

        KSException ex = new KSException("");
        ConsumerRecord<String, String> payload = new ConsumerRecord<>("topic", 1, 1, "key", "payload");
        FailoverHandler failoverHandler = Mockito.mock(FailoverHandler.class);
        KSKafkaHelper spy = spy(ksKafkaHelper);
        doReturn(List.of()).when(spy).getIgnoredExceptionClasses(ksConsumer1);

        doThrow(RuntimeException.class).when(applicationContext).getBean(ksConsumer1.getFailover().getHandlerBeanName(), FailoverHandler.class);

        Throwable throwable = catchThrowable(() -> spy.invokeFailoverHandler(ksConsumer1, payload, ex));
        assertThat(throwable).isInstanceOf(KSException.class);
        verify(failoverHandler, never()).handle(ksConsumer1, payload, ex);
    }

    @Test
    void it_should_apply_factory_settings() throws ClassNotFoundException {
        KSConsumer ksConsumer1 = new KSConsumer();
        KSConsumerFailover failover = new KSConsumerFailover();
        failover.setHandlerBeanName("handler");
        ksConsumer1.setFailover(failover);
        ksConsumer.setErrorProducerName("default");
        given(ksSenderDelegate.checkProducer(ksConsumer.getErrorProducerName())).willReturn(true);

        //when
        ConsumerFactory springConsumerFactory = ksKafkaHelper.createSpringConsumerFactory(ksConsumer, Class.forName(ksConsumer.getDataClass()), ksConsumer.getProps(), ksConsumer.getFactoryProps());
        //when
        ConcurrentKafkaListenerContainerFactory springKafkaListenerContainerFactory = ksKafkaHelper.createSpringKafkaListenerContainerFactory(springConsumerFactory, ksConsumer, ksConsumer.getFactoryProps());

        ksKafkaHelper.applyFactorySettings(springConsumerFactory, ksConsumer1, configurationProperties.getSharedFactoryProps().getConsumer(), springKafkaListenerContainerFactory);
        assertThat(springKafkaListenerContainerFactory.getContainerProperties().getAckMode()).isEqualTo(
                configurationProperties.getSharedFactoryProps().getConsumer().getAckMode()
        );
    }




    enum ConsumerType {
        JSON, AVRO, PROTO
    }

    public record DemoModel(
            String name,
            Integer age
    ) {
    }
}