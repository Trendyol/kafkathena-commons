package com.trendyol.mpc.kafkathena.commons.handler;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trendyol.mpc.kafkathena.commons.model.KSConsumer;
import com.trendyol.mpc.kafkathena.commons.model.KSExponentialRetry;
import com.trendyol.mpc.kafkathena.commons.model.constant.KSHeader;
import com.trendyol.mpc.kafkathena.commons.model.constant.KSType;
import com.trendyol.mpc.kafkathena.commons.model.exception.KSException;
import com.trendyol.mpc.kafkathena.commons.model.exception.KSIgnoredException;
import com.trendyol.mpc.kafkathena.commons.model.sharedfactory.KSSharedConsumerFactoryProperties;
import com.trendyol.mpc.kafkathena.commons.util.extensions.ExceptionSupport;
import com.trendyol.mpc.kafkathena.commons.util.extensions.KSJsonSupport;
import com.trendyol.mpc.kafkathena.commons.util.extensions.KSMapSupport;
import com.trendyol.mpc.kafkathena.commons.util.filterstrategy.KSRecordFilterStrategy;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.kafka.support.mapping.DefaultJackson2JavaTypeMapper;
import org.springframework.kafka.support.mapping.Jackson2JavaTypeMapper;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.stereotype.Component;
import org.springframework.util.backoff.FixedBackOff;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;

/**
 * Utilities for interacting with Apache Kafka
 *
 * @author sercan.celenk
 * @since first version
 */

@Component
@RequiredArgsConstructor
@Log4j2
public class KSKafkaHelper implements KSMapSupport, KSJsonSupport, ExceptionSupport {
    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private final ApplicationContext applicationContext;
    private final KSSenderDelegate ksSenderDelegate;

    /**
     * Apply Consumer Filter if defined
     *
     * @param consumer
     * @param factory
     * @param <T>
     */
    private static <T> void applyFilterMechanismToFactory(KSConsumer consumer, ConcurrentKafkaListenerContainerFactory<String, T> factory) {
        Optional.ofNullable(consumer.getFilterHeader())
                .flatMap(filterHeader -> Optional.ofNullable(filterHeader.getConsumerFilterKey()))
                .ifPresent(filterKey -> factory.setRecordFilterStrategy(new KSRecordFilterStrategy(filterKey)));
    }

    @NotNull
    private static ExponentialBackOffWithMaxRetries createExponentialRetry(KSConsumer consumer, KSExponentialRetry retryConf) {
        ExponentialBackOffWithMaxRetries exponentialRetry = new ExponentialBackOffWithMaxRetries(Optional.of(retryConf.getRetryCount()).orElseThrow(() -> new KSException("missing.exponential.retry.count for consumer topic: " + consumer.getTopic())));
        exponentialRetry.setInitialInterval(Optional.ofNullable(retryConf.getBackoffIntervalMillis()).orElseThrow(() -> new KSException("missing.exponential.backoffintervalmills for consumer topic: " + consumer.getTopic())));
        exponentialRetry.setMultiplier(Optional.ofNullable(retryConf.getMultiplier()).orElseThrow(() -> new KSException("missing.exponential.multiplier for consumer topic: " + consumer.getTopic())));
        exponentialRetry.setMaxInterval(Optional.ofNullable(retryConf.getMaxInterval()).orElseThrow(() -> new KSException("missing.exponential.max.interval for consumer topic: " + consumer.getTopic())));
        return exponentialRetry;
    }

    public <T> ConsumerFactory<String, T> createSpringConsumerFactory(KSConsumer consumer, Class<T> classT, Map<String, Object> appliedConsumerProperties, KSSharedConsumerFactoryProperties mergedFactoryProps) {
        Optional.ofNullable(mergedFactoryProps.getInterceptor()).ifPresent(interceptorClassPath -> appliedConsumerProperties.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptorClassPath));
        if (KSType.AVRO.name().equals(consumer.getType()) || KSType.PROTOBUF.name().equals(consumer.getType())) {
            appliedConsumerProperties.put("spring.json.trusted.packages", "*");
            return new DefaultKafkaConsumerFactory<>(appliedConsumerProperties);
        } else {
            DefaultJackson2JavaTypeMapper typeMapper = new DefaultJackson2JavaTypeMapper();
            typeMapper.setTypePrecedence(Jackson2JavaTypeMapper.TypePrecedence.INFERRED);

            var objectMapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            ErrorHandlingDeserializer<String> keyDeserializer = new ErrorHandlingDeserializer<>(new StringDeserializer());
            var kafkaSmartConfigJsonDeserializer = new JsonDeserializer<>(classT, objectMapper);
            kafkaSmartConfigJsonDeserializer.setTypeMapper(typeMapper);
            var valueDeserializer = new ErrorHandlingDeserializer<>(kafkaSmartConfigJsonDeserializer);
            return new DefaultKafkaConsumerFactory<>(appliedConsumerProperties, keyDeserializer, valueDeserializer);
        }
    }

    /**
     * Create a listener factory
     *
     * @param consumerFactory
     * @param consumer
     * @param mergedSharedFactoryProps
     * @param <T>
     * @return
     */
    public <T> ConcurrentKafkaListenerContainerFactory<String, T> createSpringKafkaListenerContainerFactory(
            ConsumerFactory<String, T> consumerFactory,
            KSConsumer consumer,
            KSSharedConsumerFactoryProperties mergedSharedFactoryProps
    ) {
        ConcurrentKafkaListenerContainerFactory<String, T> factory = new ConcurrentKafkaListenerContainerFactory<>();

        applyFactorySettings(consumerFactory, consumer, mergedSharedFactoryProps, factory);

        applyRetryMechanismToFactory(consumer, factory);

        applyFilterMechanismToFactory(consumer, factory);

        return factory;
    }

    /**
     * Apply configurations to listener factory
     *
     * @param consumerFactory
     * @param consumer
     * @param ksSharedConsumerFactoryProperties
     * @param factory
     * @param <T>
     */
    public <T> void applyFactorySettings(ConsumerFactory<String, T> consumerFactory, KSConsumer consumer, KSSharedConsumerFactoryProperties ksSharedConsumerFactoryProperties, ConcurrentKafkaListenerContainerFactory<String, T> factory) {
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setMissingTopicsFatal(Optional.ofNullable(ksSharedConsumerFactoryProperties.getMissingTopicAlertEnable()).orElse(false));
        factory.getContainerProperties().setAckMode(Optional.ofNullable(ksSharedConsumerFactoryProperties.getAckMode()).orElse(ContainerProperties.AckMode.RECORD));
        factory.getContainerProperties().setSyncCommits(Optional.ofNullable(ksSharedConsumerFactoryProperties.getSyncCommit()).orElse(true));
        factory.getContainerProperties().setSyncCommitTimeout(Duration.ofSeconds(Optional.ofNullable(ksSharedConsumerFactoryProperties.getSyncCommitTimeoutSecond()).orElse(5)));
        factory.setConcurrency(Optional.ofNullable(ksSharedConsumerFactoryProperties.getConcurrency()).orElse(1));
        factory.setAutoStartup(Optional.ofNullable(ksSharedConsumerFactoryProperties.getAutoStartup()).orElse(true));
        factory.setBatchListener(ksSharedConsumerFactoryProperties.getBatch());
        Properties properties = new Properties();
        properties.putAll(consumerFactory.getConfigurationProperties());
        factory.getContainerProperties().setKafkaConsumerProperties(properties);
        log.debug("Kafkathena: Consumer: '{}' - Factory settings are applied.", consumer.getName());
    }

    /**
     * Apply retry mechanism if defined
     *
     * @param consumer
     * @param factory
     * @param <T>
     */
    public <T> void applyRetryMechanismToFactory(KSConsumer consumer, ConcurrentKafkaListenerContainerFactory<String, T> factory) {
        if (Optional.ofNullable(consumer.getExponentialRetry()).isPresent() && Optional.ofNullable(consumer.getFixedRetry()).isPresent()) {
            throw new KSException("multiple.retry.config.detected.please.choose.fixed.or.exponential");
        }

        Optional.ofNullable(consumer.getExponentialRetry())
                .ifPresentOrElse(retryConf -> {
                            checkProducerDefinitions(consumer);
                            ExponentialBackOffWithMaxRetries exponentialRetry = createExponentialRetry(consumer, retryConf);
                            DefaultErrorHandler defaultErrorHandler = new DefaultErrorHandler(getFailoverRecoverer(consumer), exponentialRetry);
                            defaultErrorHandler.addNotRetryableExceptions(KSIgnoredException.class);

                            getIgnoredExceptionClasses(consumer)
                                    .forEach(defaultErrorHandler::addNotRetryableExceptions);
                            factory.setCommonErrorHandler(defaultErrorHandler);

                            log.debug("Kafkathena: Consumer: '{}' - Exponential retry is configured with {}", consumer.getName(), exponentialRetry);
                        },
                        () -> log.debug("Kafkathena: Consumer: '{}' - exponential.retry.not.define. Ignoring.", consumer.getName()));

        Optional.ofNullable(consumer.getFixedRetry())
                .ifPresentOrElse(retryConf -> {
                            checkProducerDefinitions(consumer);
                            FixedBackOff fixedBackOff = new FixedBackOff(Optional.of(retryConf.getBackoffIntervalMillis()).orElseThrow(() -> new KSException("missing.fixed.retry.backoffintervalmills for consumer topic: " + consumer.getTopic())),
                                    Optional.of(retryConf.getRetryCount()).orElseThrow(() -> new KSException("missing.fixed.retry.count for consumer topic: " + consumer.getTopic())));
                            DefaultErrorHandler defaultErrorHandler = new DefaultErrorHandler(getFailoverRecoverer(consumer), fixedBackOff);
                            defaultErrorHandler.addNotRetryableExceptions(KSIgnoredException.class);
                            getIgnoredExceptionClasses(consumer)
                                    .forEach(defaultErrorHandler::addNotRetryableExceptions);
                            factory.setCommonErrorHandler(defaultErrorHandler);
                            log.debug("Kafkathena: Consumer: '{}' - Fixed retry is configured with {}", consumer.getName(), fixedBackOff);
                        },
                        () -> log.debug("Kafkathena: Consumer: '{}' - fixed.retry.not.define. Ignoring.", consumer.getName()));
    }

    /**
     * Create Consumer Record Recoverer function with given consumer
     *
     * @param consumer
     * @return ConsumerRecordRecoverer
     * @throws KSException
     */
    public ConsumerRecordRecoverer getFailoverRecoverer(KSConsumer consumer) {
        return (payload, exception) -> {
            invokeErrorTopicAction(consumer, payload, exception);
            invokeFailoverHandler(consumer, payload, exception);
        };
    }

    public void invokeErrorTopicAction(KSConsumer consumer, ConsumerRecord<?, ?> payload, Exception exception) {
        Optional.ofNullable(consumer.getFailover())
                .flatMap(failover -> Optional.ofNullable(failover.getErrorTopic()))
                .ifPresent(errorTopic -> {

                    try {
                        if (isIgnoredException(consumer, exception)) {
                            log.debug("Kafkathena Send Error Topic Consumer exception ignoring by configuration. {}", exception.getMessage(), exception);
                        } else {
                            ProducerRecord<String, Object> producerRecord = new ProducerRecord<>(errorTopic, (Objects.isNull(payload.key()) ? UUID.randomUUID().toString() : payload.key().toString()), payload.value());
                            Optional.ofNullable(consumer.getFilterHeader())
                                    .ifPresent(filterHeader -> {
                                        Optional.ofNullable(filterHeader.getErrorProducerFilterKey())
                                                .ifPresent(filterKey ->
                                                        producerRecord.headers().add(KSHeader.FILTER_KEY.getHeaderKeyName(), filterKey.getBytes(StandardCharsets.UTF_8)));
                                        if (CollectionUtils.isNotEmpty(filterHeader.getCustomHeaders())) {
                                            consumer.getFilterHeader()
                                                    .getCustomHeaders()
                                                    .stream().filter(customHeader -> Objects.nonNull(customHeader.key()) && Objects.nonNull(customHeader.value()))
                                                    .forEach(customHeader -> producerRecord.headers().add(customHeader.key(), customHeader.value().getBytes(StandardCharsets.UTF_8)));
                                        }
                                    });

                            ksSenderDelegate.send(consumer.getErrorProducerName(), producerRecord);
                            log.info("Kafkathena: Sending error to error topic succeedded. Exception: {}, Class: {}, Payload: {}", exception, Optional.ofNullable(consumer.getDataClass()).map(a -> a.substring(consumer.getDataClass().lastIndexOf(".") + 1)).orElse(""), payload.value());
                        }
                    } catch (Exception e) {
                        log.error("Kafkathena: Consumer Failover has an error while sending error to error topic. topic: {}, key: {}, val: {}",
                                errorTopic,
                                payload.key(),
                                asJson(OBJECT_MAPPER, payload.value()),
                                e
                        );
                    }

                });
    }

    public void invokeFailoverHandler(KSConsumer consumer, ConsumerRecord<?, ?> payload, Exception exception) {
        Optional.ofNullable(consumer.getFailover())
                .flatMap(failover -> Optional.ofNullable(failover.getHandlerBeanName()))
                .ifPresent(handlerBeanName -> {
                    if (isIgnoredException(consumer, exception)) {
                        log.debug("Kafkathena Failover Handler consumer exception ignoring by configuration. {}", exception.getMessage(), exception);
                    } else {
                        try {
                            FailoverHandler failoverService = applicationContext.getBean(handlerBeanName, FailoverHandler.class);
                            failoverService.handle(consumer, payload, exception);
                        } catch (BeansException beansException) {
                            log.error("Kafkathena: Defining failover handler bean could not be found in sprint context. {}", consumer.getFailover().getHandlerBeanName());
                            throw new KSException("failover.handler.bean.could.not.be.found");
                        } catch (Exception ex) {
                            log.error("Kafkathena: failover.handler.bean.action has an error. {}", ExceptionUtils.getRootCauseMessage(ex), ex);
                            throw new KSException("failover.handler.bean.action has an error. {}" + ExceptionUtils.getRootCauseMessage(ex));
                        }
                    }
                });
    }

    /**
     * Check producer definitions that are defined in yaml configurations
     *
     * @param consumerConfig
     * @throws KSException
     */
    public void checkProducerDefinitions(KSConsumer consumerConfig) {
        Optional.ofNullable(consumerConfig.getFailover())
                .flatMap(failover -> Optional.ofNullable(failover.getErrorTopic()))
                .ifPresent(errorTopic -> {
                    if (StringUtils.isEmpty(consumerConfig.getErrorProducerName())) {
                        throw new KSException("Please define a producer name on consumer config. " + consumerConfig.getFactoryBeanName());
                    }
                    boolean isProducerDefined = ksSenderDelegate.checkProducer(consumerConfig.getErrorProducerName());
                    if (!isProducerDefined) {
                        throw new KSException("Producer is not defined on producers configs. " + consumerConfig.getFactoryBeanName() + " Producer Name: " + consumerConfig.getErrorProducerName());
                    }
                });
    }
}
