package com.trendyol.mpc.kafkathena.commons.util;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trendyol.mpc.kafkathena.commons.exception.KSException;
import com.trendyol.mpc.kafkathena.commons.model.KSConsumer;
import com.trendyol.mpc.kafkathena.commons.model.KSConsumerFactoryProp;
import com.trendyol.mpc.kafkathena.commons.model.KSConsumerProducerProp;
import com.trendyol.mpc.kafkathena.commons.model.constant.KSHeader;
import com.trendyol.mpc.kafkathena.commons.model.constant.KSType;
import com.trendyol.mpc.kafkathena.commons.util.extensions.FailoverHandler;
import com.trendyol.mpc.kafkathena.commons.util.extensions.KSJsonSupport;
import com.trendyol.mpc.kafkathena.commons.util.extensions.KSMapSupport;
import com.trendyol.mpc.kafkathena.commons.util.filterstrategy.KSRecordFilterStrategy;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.kafka.support.mapping.DefaultJackson2JavaTypeMapper;
import org.springframework.kafka.support.mapping.Jackson2JavaTypeMapper;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.stereotype.Component;
import org.springframework.util.backoff.FixedBackOff;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;

@Component
@RequiredArgsConstructor
@Log4j2
public class KSKafkaHelper implements KSMapSupport, KSJsonSupport {
    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private final KSConsumerProducerProp consumerProps;
    private final ApplicationContext applicationContext;
    private final KSSenderDelegate ksSenderDelegate;

    public <T> ConsumerFactory<String, T> createSpringConsumerFactory(KSConsumer consumer, Class<T> classT, KSConsumerFactoryProp ksConsumerFactoryProp) throws IOException {
        Map<String, Object> consumerProperties = consumer.getProps();
        Optional.ofNullable(ksConsumerFactoryProp.getInterceptorClassPath()).ifPresent(interceptorClassPath -> consumerProperties.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptorClassPath));
        Map<String, Object> mergedProps = mergeKafkaProps(consumerProperties, consumerProps.getConsumerPropsDefaults());

        if (KSType.AVRO.name().equals(consumer.getType()) || KSType.PROTOBUF.name().equals(consumer.getType())) {
            mergedProps.put("spring.json.trusted.packages", "*");
            return new DefaultKafkaConsumerFactory<String, T>(mergedProps);
        } else {
            DefaultJackson2JavaTypeMapper typeMapper = new DefaultJackson2JavaTypeMapper();
            typeMapper.setTypePrecedence(Jackson2JavaTypeMapper.TypePrecedence.INFERRED);

            var objectMapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            ErrorHandlingDeserializer<String> keyDeserializer = new ErrorHandlingDeserializer<>(new StringDeserializer());
            var kafkaSmartConfigJsonDeserializer = new JsonDeserializer<>(classT, objectMapper);
            kafkaSmartConfigJsonDeserializer.setTypeMapper(typeMapper);
            var valueDeserializer = new ErrorHandlingDeserializer<>(kafkaSmartConfigJsonDeserializer);
            return new DefaultKafkaConsumerFactory<String, T>(mergedProps, (Deserializer<String>) keyDeserializer, (Deserializer<T>) valueDeserializer);
        }

    }

    public <T> ConcurrentKafkaListenerContainerFactory<String, T> createSpringKafkaListenerContainerFactory(
            ConsumerFactory<String, T> consumerFactory,
            KSConsumer consumer,
            KSConsumerFactoryProp ksConsumerFactoryProp
    ) {
        ConcurrentKafkaListenerContainerFactory<String, T> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setMissingTopicsFatal(ksConsumerFactoryProp.getMissingTopicAlertEnable());
        factory.getContainerProperties().setAckMode(ksConsumerFactoryProp.getAckMode());
        factory.getContainerProperties().setSyncCommits(ksConsumerFactoryProp.getSyncCommit());
        factory.getContainerProperties().setSyncCommitTimeout(Duration.ofSeconds(ksConsumerFactoryProp.getSyncCommitTimeoutSecond()));
        factory.setConcurrency(ksConsumerFactoryProp.getConcurrency());
        factory.setAutoStartup(ksConsumerFactoryProp.getAutoStartup());

        Properties consumerProperties = new Properties();
        consumerProperties.putAll(mergeKafkaProps(consumer.getProps(), consumerProps.getConsumerPropsDefaults()));
        factory.getContainerProperties().setKafkaConsumerProperties(consumerProperties);

        if (Optional.ofNullable(consumer.getExponentialRetry()).isPresent() && Optional.ofNullable(consumer.getFixedRetry()).isPresent()) {
            throw new RuntimeException("multiple.retry.config.detected.please.choose.fixed.or.exponential");
        }

        Optional.ofNullable(consumer.getExponentialRetry())
                .ifPresentOrElse(retryConf -> {
                            checkProducerDefinitions(consumer);
                            ExponentialBackOffWithMaxRetries exponentialBackOffWithMaxRetries = new ExponentialBackOffWithMaxRetries(Optional.of(retryConf.getRetryCount()).orElseThrow(() -> new RuntimeException("missing.exponential.retry.count for consumer topic: " + consumer.getTopic())));
                            exponentialBackOffWithMaxRetries.setInitialInterval(Optional.ofNullable(retryConf.getBackoffIntervalMillis()).orElseThrow(() -> new RuntimeException("missing.exponential.backoffintervalmills for consumer topic: " + consumer.getTopic())));
                            exponentialBackOffWithMaxRetries.setMultiplier(Optional.ofNullable(retryConf.getMultiplier()).orElseThrow(() -> new RuntimeException("missing.exponential.multiplier for consumer topic: " + consumer.getTopic())));
                            exponentialBackOffWithMaxRetries.setMaxInterval(Optional.ofNullable(retryConf.getMaxInterval()).orElseThrow(() -> new RuntimeException("missing.exponential.max.interval for consumer topic: " + consumer.getTopic())));
                            factory.setCommonErrorHandler(new DefaultErrorHandler(getFailoverRecoverer(consumer), exponentialBackOffWithMaxRetries));
                        },
                        () -> log.warn("exponential.retry.not.define for consumer topic: {}", consumer.getTopic()));

        Optional.ofNullable(consumer.getFixedRetry())
                .ifPresentOrElse(retryConf -> {
                            checkProducerDefinitions(consumer);
                            FixedBackOff fixedBackOff = new FixedBackOff(Optional.of(retryConf.getBackoffIntervalMillis()).orElseThrow(() -> new RuntimeException("missing.fixed.retry.backoffintervalmills for consumer topic: " + consumer.getTopic())),
                                    Optional.of(retryConf.getRetryCount()).orElseThrow(() -> new RuntimeException("missing.fixed.retry.count for consumer topic: " + consumer.getTopic())));
                            DefaultErrorHandler defaultErrorHandler = new DefaultErrorHandler(getFailoverRecoverer(consumer), fixedBackOff);
                            Class<Exception>[] ignoredExceptionClasses = getIgnoredExceptionClasses(consumer);
                            defaultErrorHandler.addNotRetryableExceptions(ignoredExceptionClasses);
                            factory.setCommonErrorHandler(defaultErrorHandler);
                        },
                        () -> log.warn("fixed.retry.not.define for consumer topic: {}", consumer.getTopic()));

        Optional.ofNullable(consumer.getFilterHeader())
                .flatMap(filterHeader -> Optional.ofNullable(filterHeader.getConsumerFilterKey()))
                .ifPresent(filterKey -> factory.setRecordFilterStrategy(new KSRecordFilterStrategy(filterKey)));

        return factory;
    }

    public ConsumerRecordRecoverer getFailoverRecoverer(KSConsumer consumer) {
        return (record, exception) -> {
            Optional.ofNullable(consumer.getFailover())
                    .flatMap(failover -> Optional.ofNullable(failover.getHandlerBeanName()))
                    .ifPresent(_any -> {
                        try {
                            FailoverHandler failoverService = applicationContext.getBean(consumer.getFailover().getHandlerBeanName(), FailoverHandler.class);
                            failoverService.handle(consumer, record, exception);
                        }catch (Exception ex){
                            log.warn("Kafkathena Failover Handler invoke has an exception. Consumer Exception: {}, Class: {}, Payload: {}", exception.getMessage(), Optional.ofNullable(consumer.getDataClass()).map(a -> a.substring(consumer.getDataClass().lastIndexOf(".") + 1)).orElse(""), record.value(), ex);
                        }
                    });

            Optional.ofNullable(consumer.getFailover())
                    .flatMap(failover -> Optional.ofNullable(failover.getErrorTopic()))
                    .ifPresent(_any -> {
                        try {
                            ProducerRecord<String, Object> producerRecord = new ProducerRecord<>(consumer.getFailover().getErrorTopic(), (Objects.isNull(record.key()) ? UUID.randomUUID().toString() : record.key().toString()), record.value());
                            Optional.ofNullable(consumer.getFilterHeader())
                                    .flatMap(filterHeader -> Optional.ofNullable(filterHeader.getErrorProducerFilterKey()))
                                    .ifPresent(filterKey -> producerRecord.headers().add(KSHeader.FILTER_KEY.getHeaderKeyName(), filterKey.getBytes(StandardCharsets.UTF_8)));

                            ksSenderDelegate.send(consumer.getErrorProducerName(), producerRecord);
                            log.info("Kafkathena Sending error to error topic succeedded. Exception: {}, Class: {}, Payload: {}", exception, Optional.ofNullable(consumer.getDataClass()).map(a -> a.substring(consumer.getDataClass().lastIndexOf(".") + 1)).orElse(""), record.value());
                        } catch (Exception e) {
                            log.error("Consumer Failover has an error while sending error to error topic. topic: {}, key: {}, val: {}",
                                    consumer.getFailover().getErrorTopic(),
                                    record.key(),
                                    asJson(OBJECT_MAPPER, record.value()),
                                    e
                            );
                        }
                    });
        };
    }

    public void checkProducerDefinitions(KSConsumer consumerConfig) {
        Optional.ofNullable(consumerConfig.getFailover())
                .flatMap(failover -> Optional.ofNullable(failover.getErrorTopic()))
                .ifPresent(errorTopic -> {
                    if (StringUtils.isEmpty(consumerConfig.getErrorProducerName())) {
                        throw new RuntimeException("Please define a producer name on consumer config. " + consumerConfig.getFactoryBeanName());
                    }
                    boolean isProducerDefined = ksSenderDelegate.checkProducer(consumerConfig.getErrorProducerName());
                    if (!isProducerDefined) {
                        throw new RuntimeException("Producer is not defined on producers configs. " + consumerConfig.getFactoryBeanName() + " Producer Name: " + consumerConfig.getErrorProducerName());
                    }
                });
    }

    @SuppressWarnings("unchecked")
    public <T> Class<T>[] getIgnoredExceptionClasses(KSConsumer consumer) {
        List<Class<? extends Exception>> classes = new ArrayList<>();
        if(Objects.nonNull(consumer.getFailover()) && CollectionUtils.isNotEmpty(consumer.getFailover().getIgnoredExceptionClasses())){
            for(int i = 0; i<consumer.getFailover().getIgnoredExceptionClasses().size(); i++){
                String clazz = consumer.getFailover().getIgnoredExceptionClasses().get(i);
                classes.add(getExceptionClass(clazz, consumer.getFactoryBeanName()));
            }
        }
        return classes.toArray(new Class[0]);
    }

    public Class<? extends Exception> getExceptionClass(String className, String consumerFactoryName) {
        try {
            Class<?> clazz = Class.forName(className);
            if (!Throwable.class.isAssignableFrom(clazz)) {
                throw KSException.builder()
                        .message("Ignored Exception class not found for [" + className + "] being defined in kafkathena configuration. Consumer Factory Bean Name: " + consumerFactoryName).build();
            }
            return (Class<? extends Exception>) clazz;
        } catch (ClassNotFoundException e) {
            // handling the exception
            throw KSException.builder()
                    .message("Ignored Exception class not found for [" + className + "] being defined in kafkathena configuration. Consumer Factory Bean Name: " + consumerFactoryName).build();
        }
    }
}
