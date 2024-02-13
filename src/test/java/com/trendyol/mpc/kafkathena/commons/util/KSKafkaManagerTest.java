package com.trendyol.mpc.kafkathena.commons.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.trendyol.mpc.kafkathena.commons.handler.KSKafkaHelper;
import com.trendyol.mpc.kafkathena.commons.handler.KSKafkaManager;
import com.trendyol.mpc.kafkathena.commons.handler.KafkathenaConfigurationHelper;
import com.trendyol.mpc.kafkathena.commons.model.*;
import com.trendyol.mpc.kafkathena.commons.model.sharedfactory.KSSharedConsumerFactoryProperties;
import com.trendyol.mpc.kafkathena.commons.model.constant.KSType;
import com.trendyol.mpc.kafkathena.commons.model.exception.KSException;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
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
import org.springframework.kafka.listener.ContainerProperties;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class KSKafkaManagerTest {
    @InjectMocks
    KSKafkaManager ksKafkaManager;
    @Mock
    KSConfigurationProperties configurationProperties;
    @Mock
    KSKafkaHelper ksKafkaHelper;
    @Mock
    ConfigurableApplicationContext applicationContext;
    KSConfigurationProperties configurationPropertiesObj;
    KSConsumer ksConsumer;
    @Mock
    ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        configurationPropertiesObj = KafkathenaConfigurationHelper.getConfigurationProperties();
        ksConsumer = KafkathenaConfigurationHelper.addConsumerToConfiguration(configurationProperties, "consumer1", "default", true, true, true, KSType.JSON, KafkathenaConfigurationHelper.RetryType.FIXED);
    }

    @SneakyThrows
    @Test
    void it_should_CreateSpringManagedFactory() {
        //given
        given(configurationProperties.getSharedFactoryProps()).willReturn(configurationPropertiesObj.getSharedFactoryProps());
        given(configurationProperties.getConsumerPropsDefaults()).willReturn(configurationPropertiesObj.getConsumerPropsDefaults());
        given(configurationProperties.getSharedFactoryProps()).willReturn(configurationPropertiesObj.getSharedFactoryProps());

        ConsumerFactory factory = Mockito.mock(ConsumerFactory.class);
        ArgumentCaptor<Map<String, Object>> appliedConsumerPropsCaptor = ArgumentCaptor.forClass(Map.class);
        ArgumentCaptor<KSSharedConsumerFactoryProperties> mergedFactoryProps = ArgumentCaptor.forClass(KSSharedConsumerFactoryProperties.class);
        given(ksKafkaHelper.createSpringConsumerFactory(eq(ksConsumer), eq(Class.forName(ksConsumer.getDataClass())), appliedConsumerPropsCaptor.capture(), mergedFactoryProps.capture())).willReturn(factory);



        ConcurrentKafkaListenerContainerFactory listenerFactory = Mockito.mock(ConcurrentKafkaListenerContainerFactory.class);
        given(ksKafkaHelper.createSpringKafkaListenerContainerFactory(eq(factory), eq(ksConsumer), mergedFactoryProps.capture())).willReturn(listenerFactory);
        ConfigurableListableBeanFactory context = Mockito.mock(DefaultListableBeanFactory.class);
        given((applicationContext).getBeanFactory()).willReturn(context);
        ArgumentCaptor<ConcurrentKafkaListenerContainerFactory<String, ?>> concurrentKafkaListenerContainerFactoryArgumentCaptor = ArgumentCaptor.forClass(ConcurrentKafkaListenerContainerFactory.class);

        //when
        ksKafkaManager.createAndPublishListenerFactory(ksConsumer);
        //then
        verify(context).registerSingleton(eq("topic1FactoryBeanName"), concurrentKafkaListenerContainerFactoryArgumentCaptor.capture());
    }

    @Test
    void it_should_verify_consumer_required_props(){
        ksKafkaManager.validateConsumerRequiredProps(ksConsumer);
    }

    @Test
    void it_should_not_verify_when_not_defined_cluster(){
        ksConsumer.setCluster(null);
        Throwable throwable = catchThrowable(() -> ksKafkaManager.validateConsumerRequiredProps(ksConsumer));
        assertThat(throwable).isInstanceOf(KSException.class);
    }

    @Test
    void it_should_not_verify_when_not_defined_factory_bean_name(){
        ksConsumer.setFactoryBeanName(null);
        Throwable throwable = catchThrowable(() -> ksKafkaManager.validateConsumerRequiredProps(ksConsumer));
        assertThat(throwable).isInstanceOf(KSException.class);
    }

    @Test
    void it_should_not_verify_when_not_defined_data_class(){
        ksConsumer.setDataClass(null);
        Throwable throwable = catchThrowable(() -> ksKafkaManager.validateConsumerRequiredProps(ksConsumer));
        assertThat(throwable).isInstanceOf(KSException.class);
    }

    @Test
    void it_should_not_verify_when_not_defined_error_producer_name(){
        ksConsumer.setErrorProducerName(null);
        Throwable throwable = catchThrowable(() -> ksKafkaManager.validateConsumerRequiredProps(ksConsumer));
        assertThat(throwable).isInstanceOf(KSException.class);
    }

    @Test
    void it_should_not_verify_when_not_defined_topic(){
        ksConsumer.setTopic(null);
        Throwable throwable = catchThrowable(() -> ksKafkaManager.validateConsumerRequiredProps(ksConsumer));
        assertThat(throwable).isInstanceOf(KSException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "group.id",
            "spring.deserializer.value.delegate.class",
            "key.deserializer",
            "spring.deserializer.key.delegate.class",
            "value.deserializer"
    })
    void it_should_not_verify_when_not_defined_group_id(String config){
        ksConsumer.setType(KSType.AVRO.name());
        ksConsumer.getProps().remove(config);
        Throwable throwable = catchThrowable(() -> ksKafkaManager.validateConsumerRequiredProps(ksConsumer));
        assertThat(throwable).isInstanceOf(KSException.class);
    }

    @Test
    void it_should_applyClusterProps(){
        KSConsumer consumer = new KSConsumer();
        consumer.setCluster("confluent");
        Map<String, Object> mergedConsumerProps = new HashMap<>();
        Map<String, KSCluster> clusterMap = new HashMap<>();
        KSCluster cluster1 = new KSCluster();
        cluster1.setServers("server");
        clusterMap.put("confluent", cluster1);
        Map<String, Object> appliedClusters = ksKafkaManager.applyClusterProps(consumer, mergedConsumerProps, clusterMap);
        assertThat(appliedClusters)
                .hasSize(1)
                .containsEntry("bootstrap.servers", "server");
    }

    @Test
    void it_should_not_applyClusterProps_when_defining_cluster_is_not_exists(){
        KSConsumer consumer = new KSConsumer();
        consumer.setCluster("confluent_");
        Map<String, Object> mergedConsumerProps = new HashMap<>();
        Map<String, KSCluster> clusterMap = new HashMap<>();
        KSCluster cluster1 = new KSCluster();
        cluster1.setServers("server");
        clusterMap.put("confluent", cluster1);
        Throwable throwable = catchThrowable(() -> ksKafkaManager.applyClusterProps(consumer, mergedConsumerProps, clusterMap));
        assertThat(throwable).isInstanceOf(KSException.class);
    }

    @Test
    void it_should_validate_ok_when_class_definitions_is_ok(){
        KSConsumer consumer = new KSConsumer();
        consumer.setDataClass("com.trendyol.mpc.kafkathena.commons.model.DemoModel");
        KSConsumerFailover failover = new KSConsumerFailover();
        failover.setIgnoredExceptionClasses(List.of("com.trendyol.mpc.kafkathena.commons.model.DemoModel"));
        consumer.setFailover(failover);
        Throwable throwable = catchThrowable(() -> ksKafkaManager.validateClassReferences(consumer));
        assertThat(throwable).isNull();
    }

    @Test
    void it_should_validate_not_ok_when_data_class_definitions_is_wrong(){
        KSConsumer consumer = new KSConsumer();
        consumer.setDataClass("model.DemoModel");
        KSConsumerFailover failover = new KSConsumerFailover();
        failover.setIgnoredExceptionClasses(List.of("com.trendyol.mpc.kafkathena.commons.model.DemoModel"));
        consumer.setFailover(failover);
        Throwable throwable = catchThrowable(() -> ksKafkaManager.validateClassReferences(consumer));
        assertThat(throwable).isInstanceOf(KSException.class);
    }

    @Test
    void it_should_validate_not_ok_when_ignored_exception_class_definitions_is_wrong(){
        KSConsumer consumer = new KSConsumer();
        consumer.setDataClass("com.trendyol.mpc.kafkathena.commons.model.DemoModel");
        KSConsumerFailover failover = new KSConsumerFailover();
        failover.setIgnoredExceptionClasses(List.of("model.DemoModel"));
        consumer.setFailover(failover);
        Throwable throwable = catchThrowable(() -> ksKafkaManager.validateClassReferences(consumer));
        assertThat(throwable).isInstanceOf(KSException.class);
    }
}