package com.trendyol.mpc.kafkathena.commons.initializer;

import com.trendyol.mpc.kafkathena.commons.model.KSCluster;
import com.trendyol.mpc.kafkathena.commons.model.KSConfigurationProperties;
import com.trendyol.mpc.kafkathena.commons.model.KSProducer;
import com.trendyol.mpc.kafkathena.commons.model.exception.KSException;
import com.trendyol.mpc.kafkathena.commons.model.sharedfactory.KSSharedProducerFactoryProperties;
import com.trendyol.mpc.kafkathena.commons.util.extensions.KSMapSupport;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Getter
@Component
@RequiredArgsConstructor
@Slf4j
public class KafkathenaProducersInitializer implements KSMapSupport {
    private final KSConfigurationProperties ksConfigurationProperties;
    private final ConcurrentMap<String, KafkaTemplate<String, Object>> kafkathenaProducerMap = new ConcurrentHashMap<>();

    public ConcurrentMap<String, KafkaTemplate<String, Object>> getProducerMap() {
        return getKafkathenaProducerMap();
    }

    public KafkaTemplate<String, Object> getProducer(String name) {
        return getProducerMap().get(name);
    }

    public void initProducers() {

        Optional.ofNullable(ksConfigurationProperties.getProducers())
                .filter(MapUtils::isNotEmpty)
                .ifPresent(producers -> {
                    log.info("Kafkathena: Found {} producers", producers.size());
                    producers
                            .forEach((name, producer) -> {
                                producer.setName(name);
                                Map<String, KSCluster> clusters = ksConfigurationProperties.getSharedFactoryProps().getClusters();
                                if (MapUtils.isEmpty(clusters)) {
                                    throw new KSException("Could not find any cluster in configuration.");
                                }

                                KSSharedProducerFactoryProperties mergedProducerFactoryProps = mergeProducerFactoryProps(ksConfigurationProperties.getSharedFactoryProps().getProducer(), producer.getFactoryProps());
                                Map<String, Object> mergedProducerProps = mergeKafkaProps(producer.getProps(), ksConfigurationProperties.getProducerPropsDefaults());
                                Map<String, Object> appliedProducerProps = applyClusterProps(producer, mergedProducerProps, clusters);
                                Optional.ofNullable(mergedProducerFactoryProps.getInterceptor()).ifPresent(interceptorClassPath -> appliedProducerProps.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptorClassPath));

                                DefaultKafkaProducerFactory<String, Object> kafkaProducerFactory = new DefaultKafkaProducerFactory<>(appliedProducerProps);
                                kafkathenaProducerMap.putIfAbsent(name.concat("_kt_producer"), new KafkaTemplate<>(kafkaProducerFactory));
                                log.info("Kafkathena: Created Producer '{}' with properties: {}", producer.getName(), appliedProducerProps);
                            });
                });
    }

    public Map<String, Object> applyClusterProps(KSProducer producer, Map<String, Object> mergedProducerProps, Map<String, KSCluster> clusters) {
        KSCluster producerCluster = clusters.getOrDefault(producer.getCluster(), null);
        if (Objects.nonNull(producerCluster)) {
            String bootstrapServers = producerCluster.getServers();
            Map<String, Object> additionalProps = producerCluster.getAdditionalProps();
            mergedProducerProps.put("bootstrap.servers", bootstrapServers);
            if (MapUtils.isNotEmpty(additionalProps)) {
                mergedProducerProps.putAll(additionalProps);
            }
        } else {
            throw new KSException(String.format("Producer %s is not configured with cluster", producer.getName()));
        }
        return mergedProducerProps;
    }
}
