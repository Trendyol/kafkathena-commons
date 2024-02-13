package com.trendyol.mpc.kafkathena.commons.util;

import com.trendyol.mpc.kafkathena.commons.handler.KafkathenaConfigurationHelper;
import com.trendyol.mpc.kafkathena.commons.initializer.KafkathenaProducersInitializer;
import com.trendyol.mpc.kafkathena.commons.model.KSConfigurationProperties;
import com.trendyol.mpc.kafkathena.commons.model.KSProducer;
import com.trendyol.mpc.kafkathena.commons.model.constant.KSType;
import com.trendyol.mpc.kafkathena.commons.model.exception.KSException;
import com.trendyol.mpc.kafkathena.commons.model.sharedfactory.KSSharedFactoryProperties;
import com.trendyol.mpc.kafkathena.commons.model.sharedfactory.KSSharedProducerFactoryProperties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.BDDMockito.given;

@ExtendWith(MockitoExtension.class)
class KafkathenaProducersInitializerTest {
    @InjectMocks
    KafkathenaProducersInitializer kafkathenaProducersInitializer;
    @Mock
    KSConfigurationProperties ksConfigurationProperties;
    KSConfigurationProperties configurationProperties;


    @BeforeEach
    public void setUp() {
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
    void it_should_initialize_producers() {
        //given
        given(ksConfigurationProperties.getProducers()).willReturn(configurationProperties.getProducers());
        given(ksConfigurationProperties.getSharedFactoryProps()).willReturn(configurationProperties.getSharedFactoryProps());
        given(ksConfigurationProperties.getProducerPropsDefaults()).willReturn(configurationProperties.getProducerPropsDefaults());

        //when
        kafkathenaProducersInitializer.initProducers();

        //then
        KSProducer producer = ksConfigurationProperties.getProducers().get("default");
        Map<String, Object> producerProps = ksConfigurationProperties.getProducers().get("default").getProps();
        KafkaTemplate<String, Object> producerTemplate = kafkathenaProducersInitializer.getProducerMap().get("default_kt_producer");
        KSSharedProducerFactoryProperties mergedProducerFactoryProps = kafkathenaProducersInitializer.mergeProducerFactoryProps(ksConfigurationProperties.getSharedFactoryProps().getProducer(), producer.getFactoryProps());
        Map<String, Object> mergedProducerProps = kafkathenaProducersInitializer.mergeKafkaProps(producerProps, ksConfigurationProperties.getProducerPropsDefaults());
        Map<String, Object> appliedProducerProps = kafkathenaProducersInitializer.applyClusterProps(producer, mergedProducerProps, ksConfigurationProperties.getSharedFactoryProps().getClusters());
        Optional.ofNullable(mergedProducerFactoryProps.getInterceptor()).ifPresent(interceptorClassPath -> appliedProducerProps.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptorClassPath));
        assertThat(producer).isNotNull();
        assertThat(producerTemplate.getProducerFactory().getConfigurationProperties()).usingRecursiveComparison().isEqualTo(appliedProducerProps);
    }

    @Test
    void it_should_initialize_producers_producer_props_is_null() {
        //given
        configurationProperties.setProducers(new HashMap<>());
        KafkathenaConfigurationHelper.addProducerToConfiguration(configurationProperties, "default", "default", true);

        given(ksConfigurationProperties.getProducers()).willReturn(configurationProperties.getProducers());
        given(ksConfigurationProperties.getSharedFactoryProps()).willReturn(configurationProperties.getSharedFactoryProps());
        given(ksConfigurationProperties.getProducerPropsDefaults()).willReturn(configurationProperties.getProducerPropsDefaults());

        //when
        kafkathenaProducersInitializer.initProducers();

        //then
        KSProducer producer = ksConfigurationProperties.getProducers().get("default");
        Map<String, Object> producerProps = ksConfigurationProperties.getProducers().get("default").getProps();
        KafkaTemplate<String, Object> producerTemplate = kafkathenaProducersInitializer.getProducerMap().get("default_kt_producer");
        Map<String, Object> mergedProducerProps = kafkathenaProducersInitializer.mergeKafkaProps(producerProps, ksConfigurationProperties.getProducerPropsDefaults());
        Map<String, Object> appliedProducerProps = kafkathenaProducersInitializer.applyClusterProps(producer, mergedProducerProps, ksConfigurationProperties.getSharedFactoryProps().getClusters());
        assertThat(producer).isNotNull();
        assertThat(producerTemplate.getProducerFactory().getConfigurationProperties()).usingRecursiveComparison().isEqualTo(appliedProducerProps);
    }

    @Test
    void it_should_throw_error_when_shared_factory_props_is_null() {
        //given
        configurationProperties.setProducers(new HashMap<>());
        KafkathenaConfigurationHelper.addProducerToConfiguration(configurationProperties, "default", "default", true);
        given(ksConfigurationProperties.getSharedFactoryProps()).willReturn(new KSSharedFactoryProperties());
        given(ksConfigurationProperties.getProducers()).willReturn(configurationProperties.getProducers());

        //when
        Throwable throwable = catchThrowable(() -> kafkathenaProducersInitializer.initProducers());

        //then
        assertThat(throwable).isInstanceOf(KSException.class);
    }

    @Test
    void it_should_apply_cluster_props() {
        KSProducer ksProducer = KafkathenaConfigurationHelper.addProducerToConfiguration(configurationProperties, "default", "default", true);
        Map<String, Object> props = kafkathenaProducersInitializer.applyClusterProps(ksProducer, configurationProperties.getProducerPropsDefaults(), configurationProperties.getSharedFactoryProps().getClusters());
        assertThat(props).isNotEmpty();
    }

    @Test
    void it_should_apply_cluster_props_with_additional_props() {
        KSProducer ksProducer = KafkathenaConfigurationHelper.addProducerToConfiguration(configurationProperties, "default", "default", true);
        Map<String, Object> props = kafkathenaProducersInitializer.applyClusterProps(ksProducer, configurationProperties.getProducerPropsDefaults(), configurationProperties.getSharedFactoryProps().getClusters());
        assertThat(props).isNotEmpty();
    }

    @Test
    void it_should_throw_when_cluster_props_without_producer_cluster() {
        KSProducer ksProducer = KafkathenaConfigurationHelper.addProducerToConfiguration(configurationProperties, "default", "default", true);
        ksProducer.setCluster("none");
        Throwable throwable = catchThrowable(() -> kafkathenaProducersInitializer.applyClusterProps(ksProducer, configurationProperties.getProducerPropsDefaults(), configurationProperties.getSharedFactoryProps().getClusters()));
        assertThat(throwable).isInstanceOf(KSException.class);
    }

    @Test
    void it_should_apply_cluster_props_without_additional_props() {
        KSProducer ksProducer = KafkathenaConfigurationHelper.addProducerToConfiguration(configurationProperties, "default", "default", true);
        configurationProperties.getSharedFactoryProps().getClusters()
                .get("default").setAdditionalProps(Map.of());
        Map<String, Object> props = kafkathenaProducersInitializer.applyClusterProps(ksProducer, configurationProperties.getProducerPropsDefaults(), configurationProperties.getSharedFactoryProps().getClusters());
        assertThat(props).isNotEmpty();
    }

}