package com.trendyol.mpc.kafkathena.commons.handler;

import com.trendyol.mpc.kafkathena.commons.model.*;
import com.trendyol.mpc.kafkathena.commons.model.constant.KSType;
import com.trendyol.mpc.kafkathena.commons.model.sharedfactory.KSSharedConsumerFactoryProperties;
import com.trendyol.mpc.kafkathena.commons.model.sharedfactory.KSSharedFactoryProperties;
import com.trendyol.mpc.kafkathena.commons.model.sharedfactory.KSSharedProducerFactoryProperties;
import org.jetbrains.annotations.NotNull;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.HashMap;
import java.util.Map;

public class KafkathenaConfigurationHelper {
    public static KSConfigurationProperties getConfigurationProperties() {
        KSConfigurationProperties configurationPropertiesObj = new KSConfigurationProperties();
        Map<String, Object> consumerPropsDefaults = createConsumerSharedDefaultProps();
        configurationPropertiesObj.setConsumerPropsDefaults(consumerPropsDefaults);
        Map<String, Object> producerPropsDefaults = createProducerSharedDefaults();
        configurationPropertiesObj.setProducerPropsDefaults(producerPropsDefaults);
        KSSharedFactoryProperties sharedFactoryProps = createSharedFactoryProps();
        configurationPropertiesObj.setSharedFactoryProps(sharedFactoryProps);
        configurationPropertiesObj.setConsumers(new HashMap<>());
        configurationPropertiesObj.setProducers(new HashMap<>());
        return configurationPropertiesObj;
    }

    public static KSConsumer addConsumerToConfiguration(KSConfigurationProperties configurationProperties, String name, String cluster, boolean withDefaultProps, boolean isErrorTopic, boolean isCustomFailver, KSType consumerType, RetryType retryType) {
        KSConsumer consumer = createConsumer(name, cluster, withDefaultProps, isErrorTopic, isCustomFailver, consumerType, retryType);
        configurationProperties.getConsumers().put(name, consumer);
        return consumer;
    }

    public static KSProducer addProducerToConfiguration(KSConfigurationProperties configurationProperties, String name, String cluster, boolean withDefaults) {
        KSProducer producer = createProducer(name, cluster, withDefaults);
        configurationProperties.getProducers().put(name, producer);
        return producer;
    }

    @NotNull
    public static KSSharedFactoryProperties createSharedFactoryProps() {
        KSSharedFactoryProperties sharedFactoryProperties = new KSSharedFactoryProperties();

        KSSharedConsumerFactoryProperties sharedConsumerFactoryProps = new KSSharedConsumerFactoryProperties();
        Map<String, KSCluster> clusterMap = new HashMap<>();
        clusterMap.put("default", KSCluster.builder().servers("localhost:9092").additionalProps(Map.of("sasl", "v")).build());

        sharedConsumerFactoryProps.setConcurrency(1);
        sharedConsumerFactoryProps.setAckMode(ContainerProperties.AckMode.RECORD);
        sharedConsumerFactoryProps.setAutoStartup(true);
        sharedConsumerFactoryProps.setSyncCommit(true);
        sharedConsumerFactoryProps.setMissingTopicAlertEnable(false);
        sharedConsumerFactoryProps.setSyncCommitTimeoutSecond(1000);

        sharedFactoryProperties.setProducer(new KSSharedProducerFactoryProperties());
        sharedFactoryProperties.setConsumer(sharedConsumerFactoryProps);
        sharedFactoryProperties.setClusters(clusterMap);
        return sharedFactoryProperties;
    }

    @NotNull
    public static Map<String, Object> createProducerSharedDefaults() {
        Map<String, Object> producerPropsDefaults = new HashMap<>();
        producerPropsDefaults.put("retries", 10);
        producerPropsDefaults.put("batch.size", 5242880);
        producerPropsDefaults.put("linger.ms", 100);
        producerPropsDefaults.put("buffer.memory", 33554432);
        producerPropsDefaults.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerPropsDefaults.put("value.serializer", "org.springframework.kafka.support.serializer.JsonSerializer");
        producerPropsDefaults.put("acks", "1");
        producerPropsDefaults.put("request.timeout.ms", 30000);
        producerPropsDefaults.put("delivery.timeout.ms", 300500);
        return producerPropsDefaults;
    }

    @NotNull
    public static Map<String, Object> createConsumerSharedDefaultProps() {
        Map<String, Object> consumerPropsDefaults = new HashMap<>();
        consumerPropsDefaults.put("bootstrap.servers", "localhost:9092");
        consumerPropsDefaults.put("group.id", "group");
        consumerPropsDefaults.put("value.deserializer", "org.springframework.kafka.support.serializer.ErrorHandlingDeserializer");
        consumerPropsDefaults.put("key.deserializer", "org.springframework.kafka.support.serializer.ErrorHandlingDeserializer");
        consumerPropsDefaults.put("spring.deserializer.key.delegate.class", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerPropsDefaults.put("spring.deserializer.value.delegate.class", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerPropsDefaults.put("max.poll.records", 100);
        consumerPropsDefaults.put("max.poll.interval.ms", 300000);
        consumerPropsDefaults.put("session.timeout.ms", 300000);
        consumerPropsDefaults.put("heartbeat.interval.ms", 3000);
        consumerPropsDefaults.put("enable.auto.commit", true);
        consumerPropsDefaults.put("auto.offset.reset", "earliest");
        consumerPropsDefaults.put("fetch.max.bytes", 52428800);
        consumerPropsDefaults.put("fetch.max.wait.ms", 500);
        return consumerPropsDefaults;
    }

    public static KSConsumer createConsumer(String name, String cluster, boolean withDefaultProps, boolean isErrorTopic, boolean isCustomFailver, KSType consumerType, RetryType retryType) {
        KSConsumer consumer1 = new KSConsumer();
        consumer1.setTopic("topic1");
        consumer1.setDataClass("com.trendyol.mpc.kafkathena.commons.util.KSKafkaHelperTest$DemoModel");
        consumer1.setFactoryBeanName("topic1FactoryBeanName");
        consumer1.setType(consumerType.name());
        consumer1.setCluster("default");
        consumer1.setErrorProducerName("default");

        KSSharedConsumerFactoryProperties factoryProp = new KSSharedConsumerFactoryProperties();
        consumer1.setFactoryProps(factoryProp);

        KSConsumerFailover topic1Failover = new KSConsumerFailover();
        if (isErrorTopic) {
            topic1Failover.setErrorTopic("topic1.error");
        }
        if (isCustomFailver) {
            topic1Failover.setHandlerBeanName("topic1FailoverHandlerBeanName");
        }
        if (isErrorTopic || isCustomFailver)
            consumer1.setFailover(topic1Failover);

        if (retryType == RetryType.FIXED) {
            KSFixedRetry fixedRetry = new KSFixedRetry();
            fixedRetry.setRetryCount(1);
            fixedRetry.setBackoffIntervalMillis(4000L);
            consumer1.setFixedRetry(fixedRetry);
        }

        if (retryType == RetryType.EXPONENTIAL) {
            KSExponentialRetry exponentialRetry = new KSExponentialRetry();
            exponentialRetry.setRetryCount(1);
            exponentialRetry.setMultiplier(1.0);
            exponentialRetry.setBackoffIntervalMillis(1000L);
            exponentialRetry.setMaxInterval(100000L);
            consumer1.setExponentialRetry(exponentialRetry);
        }

        Map<String, Object> consumer1Props = new HashMap<>();
        consumer1Props.put("group.id", "group");
        consumer1Props.put("value.deserializer", "org.springframework.kafka.support.serializer.ErrorHandlingDeserializer");
        consumer1Props.put("key.deserializer", "org.springframework.kafka.support.serializer.ErrorHandlingDeserializer");
        consumer1Props.put("spring.deserializer.key.delegate.class", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer1Props.put("spring.deserializer.value.delegate.class", "org.apache.kafka.common.serialization.StringDeserializer");
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

    public static KSProducer createProducer(String name, String cluster, boolean withDefaultProps) {
        KSProducer producer = new KSProducer();
        if (!withDefaultProps) {
            Map<String, Object> props = createProducerSharedDefaults();
            producer.setProps(props);
        }
        producer.setCluster(cluster);
        producer.setName(name);
        return producer;
    }

    public enum RetryType {FIXED, EXPONENTIAL}
}
