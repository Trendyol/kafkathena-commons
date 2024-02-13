package com.trendyol.mpc.kafkathena.commons.util;

import com.trendyol.mpc.kafkathena.commons.handler.KSKafkaManager;
import com.trendyol.mpc.kafkathena.commons.handler.KafkathenaConfigurationHelper;
import com.trendyol.mpc.kafkathena.commons.initializer.KafkathenaConsumersInitializer;
import com.trendyol.mpc.kafkathena.commons.model.KSConfigurationProperties;
import com.trendyol.mpc.kafkathena.commons.model.KSConsumer;
import com.trendyol.mpc.kafkathena.commons.model.constant.KSType;
import com.trendyol.mpc.kafkathena.commons.model.sharedfactory.KSSharedFactoryProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class KafkathenaConsumersInitializerTest {
    @InjectMocks
    KafkathenaConsumersInitializer kafkathenaConsumersInitializer;
    @Mock
    KSKafkaManager ksKafkaManager;
    @Mock
    KSConfigurationProperties ksConfigurationProperties;
    KSConfigurationProperties configurationProperties;

    @BeforeEach
    void setUp() {
        configurationProperties = KafkathenaConfigurationHelper.getConfigurationProperties();
        KSSharedFactoryProperties sharedFactoryProps = KafkathenaConfigurationHelper.createSharedFactoryProps();
        Map<String, Object> consumerSharedDefaultProps = KafkathenaConfigurationHelper.createConsumerSharedDefaultProps();
        Map<String, Object> producerSharedDefaultProps = KafkathenaConfigurationHelper.createProducerSharedDefaults();
        KafkathenaConfigurationHelper.addConsumerToConfiguration(configurationProperties, "consumer", "default", false, true, true, KSType.JSON, KafkathenaConfigurationHelper.RetryType.FIXED);
        KafkathenaConfigurationHelper.addProducerToConfiguration(configurationProperties, "default", "default", false);
        configurationProperties.setSharedFactoryProps(sharedFactoryProps);
        configurationProperties.setConsumerPropsDefaults(consumerSharedDefaultProps);
        configurationProperties.setProducerPropsDefaults(producerSharedDefaultProps);
    }

    @Test
    void it_should_init_consumers() {
        //given
        given(ksConfigurationProperties.getConsumers()).willReturn(configurationProperties.getConsumers());
        ArgumentCaptor<KSConsumer> consumerArgumentCaptor = ArgumentCaptor.forClass(KSConsumer.class);
        //when
        kafkathenaConsumersInitializer.initConsumers();
        //then
        verify(ksKafkaManager).createAndPublishListenerFactory(consumerArgumentCaptor.capture());
        KSConsumer captured = consumerArgumentCaptor.getValue();
        KSConsumer inputConsumer = configurationProperties.getConsumers().get("consumer");
        assertThat(inputConsumer.getName()).isEqualTo(captured.getName());
        assertThat(inputConsumer.getDataClass()).isEqualTo(captured.getDataClass());
        assertThat(inputConsumer.getTopic()).isEqualTo(captured.getTopic());
        assertThat(inputConsumer.getFactoryBeanName()).isEqualTo(captured.getFactoryBeanName());
        assertThat(inputConsumer.getErrorProducerName()).isEqualTo(captured.getErrorProducerName());
        assertThat(inputConsumer.getType()).isEqualTo(captured.getType());
        assertThat(inputConsumer.getCluster()).isEqualTo(captured.getCluster());
        assertThat(inputConsumer.getFixedRetry()).usingRecursiveComparison().isEqualTo(captured.getFixedRetry());
        assertThat(inputConsumer.getExponentialRetry()).usingRecursiveComparison().isEqualTo(captured.getExponentialRetry());
        assertThat(inputConsumer.getFailover()).usingRecursiveComparison().isEqualTo(captured.getFailover());
        assertThat(inputConsumer.getFactoryProps()).usingRecursiveComparison().isEqualTo(captured.getFactoryProps());
        assertThat(inputConsumer.getProps()).usingRecursiveComparison().isEqualTo(captured.getProps());
    }

}