package com.trendyol.mpc.kafkathena.commons.handler;

import com.trendyol.mpc.kafkathena.commons.model.KSCluster;
import com.trendyol.mpc.kafkathena.commons.model.KSConfigurationProperties;
import com.trendyol.mpc.kafkathena.commons.model.KSConsumer;
import com.trendyol.mpc.kafkathena.commons.model.constant.KSType;
import com.trendyol.mpc.kafkathena.commons.model.exception.KSException;
import com.trendyol.mpc.kafkathena.commons.model.sharedfactory.KSSharedConsumerFactoryProperties;
import com.trendyol.mpc.kafkathena.commons.util.extensions.ExceptionSupport;
import com.trendyol.mpc.kafkathena.commons.util.extensions.KSJsonSupport;
import com.trendyol.mpc.kafkathena.commons.util.extensions.KSMapSupport;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

@Component
@EnableRetry
@RequiredArgsConstructor
@Log4j2
public class KSKafkaManager implements KSMapSupport, KSJsonSupport, ExceptionSupport {
    private final KSKafkaHelper ksKafkaHelper;
    private final KSConfigurationProperties ksConfigurationProperties;
    private final ApplicationContext applicationContext;

    private static void validateConsumerKafkaSpecificProps(KSConsumer consumer, Map<String, Object> consumerProps) {
        if (!consumerProps.containsKey("group.id")) {
            throw new KSException(String.format("Consumer Factory [%s] has not [group.id]", consumer.getFactoryBeanName()));
        }
        if (!Objects.equals(consumer.getType(), KSType.JSON.name())) {
            if (!consumerProps.containsKey("value.deserializer")) {
                throw new KSException(String.format("Consumer Factory [%s] has not [value.deserializer] for %s type", consumer.getFactoryBeanName(), consumer.getType()));
            }
            if (!consumerProps.containsKey("spring.deserializer.value.delegate.class")) {
                throw new KSException(String.format("Consumer Factory [%s] has not [spring.deserializer.value.delegate.class] for %s type", consumer.getFactoryBeanName(), consumer.getType()));
            }
            if (!consumerProps.containsKey("key.deserializer")) {
                throw new KSException(String.format("Consumer Factory [%s] has not [key.deserializer] for %s type", consumer.getFactoryBeanName(), consumer.getType()));
            }
            if (!consumerProps.containsKey("spring.deserializer.key.delegate.class")) {
                throw new KSException(String.format("Consumer Factory [%s] has not [spring.deserializer.key.delegate.class] for %s type", consumer.getFactoryBeanName(), consumer.getType()));
            }
        }
    }

    private static void validateConsumerFailover(KSConsumer consumer) {
        if (Objects.nonNull(consumer.getFailover()) && StringUtils.isNotEmpty(consumer.getFailover().getErrorTopic()) && StringUtils.isEmpty(consumer.getErrorProducerName())) {
            throw new KSException(String.format("Consumer [%s] has not 'error-producer-name' field", consumer.getName()));
        }
    }

    private static void validateConsumerFactoryProps(KSConsumer consumer) {
        if (StringUtils.isEmpty(consumer.getCluster())) {
            throw new KSException(String.format("Consumer [%s] has not 'cluster' field", consumer.getName()));
        }
        if (StringUtils.isEmpty(consumer.getTopic())) {
            throw new KSException(String.format("Consumer [%s] has not 'topic' field", consumer.getName()));
        }
        if (StringUtils.isEmpty(consumer.getFactoryBeanName())) {
            throw new KSException(String.format("Consumer [%s] has not 'factory-bean-name' field", consumer.getName()));
        }
        if (StringUtils.isEmpty(consumer.getDataClass())) {
            throw new KSException(String.format("Consumer [%s] has not 'data-class' field", consumer.getName()));
        }
    }

    @SneakyThrows
    public void createAndPublishListenerFactory(KSConsumer consumer) {
        validateConsumerRequiredProps(consumer);
        Class<?> clz = Class.forName(consumer.getDataClass());
        KSSharedConsumerFactoryProperties mergedFactoryProps = mergeConsumerFactoryProps(ksConfigurationProperties.getSharedFactoryProps().getConsumer(), consumer.getFactoryProps());
        Map<String, Object> mergedConsumerProps = mergeKafkaProps(consumer.getProps(), ksConfigurationProperties.getConsumerPropsDefaults());
        Map<String, Object> appliedConsumerProps = applyClusterProps(consumer, mergedConsumerProps, ksConfigurationProperties.getSharedFactoryProps().getClusters());

        ConsumerFactory<String, ?> consumerFactory = ksKafkaHelper.createSpringConsumerFactory(consumer, clz, appliedConsumerProps, mergedFactoryProps);

        ConcurrentKafkaListenerContainerFactory<String, ?> kafkaSingleListenerContainerFactory = ksKafkaHelper.createSpringKafkaListenerContainerFactory(consumerFactory, consumer, mergedFactoryProps);

        ConfigurableListableBeanFactory beanFactory = ((ConfigurableApplicationContext) applicationContext).getBeanFactory();
        beanFactory.registerSingleton(consumer.getFactoryBeanName(), kafkaSingleListenerContainerFactory);

        logConsumer(consumer, consumerFactory, mergedFactoryProps);
    }

    private static void logConsumer(KSConsumer consumer, ConsumerFactory<String, ?> consumerFactory, KSSharedConsumerFactoryProperties mergedFactoryProps) {
        log.info("Kafkathena: Consumer: '{}'", consumer.getName());
        log.info("Kafkathena: Consumer: '{}' Container Properties: \n{}", consumer.getName(), consumerFactory.getConfigurationProperties().entrySet().stream().map(entry -> entry.getKey() + ": " + entry.getValue()).collect(Collectors.joining("\n")));
        log.info("Kafkathena: Consumer '{}': Factory properties: \n{}", consumer.getName(), mergedFactoryProps);
        log.info("Kafkathena: Consumer '{}' successfully created\n", consumer.getName());
    }

    public void validateConsumerRequiredProps(KSConsumer consumer) {
        Map<String, Object> consumerProps = mergeKafkaProps(consumer.getProps(), ksConfigurationProperties.getConsumerPropsDefaults());
        validateClassReferences(consumer);
        validateConsumerFactoryProps(consumer);
        validateConsumerFailover(consumer);
        validateConsumerKafkaSpecificProps(consumer, consumerProps);
    }

    public void validateClassReferences(KSConsumer consumer) {
        Optional.ofNullable(consumer.getDataClass())
                        .ifPresent(dataClass -> loadClass(consumer.getName(), dataClass));

        Optional.ofNullable(consumer.getFailover())
                .stream()
                .filter(failover -> CollectionUtils.isNotEmpty(failover.getIgnoredExceptionClasses()))
                .flatMap(failover -> failover.getIgnoredExceptionClasses().stream())
                .forEach(clazz -> loadClass(consumer.getName(), clazz));
    }

    public Map<String, Object> applyClusterProps(KSConsumer consumer, Map<String, Object> mergedProps, Map<String, KSCluster> clusters) {
        KSCluster cluster = clusters.getOrDefault(consumer.getCluster(), null);
        applyClusterProperties(cluster, mergedProps, consumer);
        log.debug("Kafkathena: Applied cluster props. {}", mergedProps);
        return mergedProps;
    }

    private void applyClusterProperties(KSCluster cluster, Map<String, Object> mergedProps, KSConsumer consumer) {
        if (cluster != null) {
            String bootstrapServers = cluster.getServers();
            Map<String, Object> additionalProps = cluster.getAdditionalProps();
            mergedProps.put("bootstrap.servers", bootstrapServers);
            if (MapUtils.isNotEmpty(additionalProps)) {
                mergedProps.putAll(additionalProps);
            }
        } else {
            throw new KSException(String.format("Consumer %s is not configured with a cluster", consumer.getName()));
        }
    }

}
