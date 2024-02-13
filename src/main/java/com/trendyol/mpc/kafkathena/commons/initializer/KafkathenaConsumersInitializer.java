package com.trendyol.mpc.kafkathena.commons.initializer;

import com.trendyol.mpc.kafkathena.commons.handler.KSKafkaManager;
import com.trendyol.mpc.kafkathena.commons.model.KSConfigurationProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
@RequiredArgsConstructor
@Slf4j
public class KafkathenaConsumersInitializer {
    private final KSKafkaManager ksKafkaManager;
    private final KSConfigurationProperties ksConfigurationProperties;

    public void initConsumers() {
        Optional.ofNullable(ksConfigurationProperties.getConsumers())
                .filter(MapUtils::isNotEmpty)
                .ifPresent(consumers -> {
                    log.info("Kafkathena: Found {} consumers in configuration.", consumers.size());
                    consumers.forEach((consumerName, consumerInfo) -> {
                        consumerInfo.setName(consumerName);
                        ksKafkaManager.createAndPublishListenerFactory(consumerInfo);
                    });
                });
        log.info("Kafkathena: Successfully initialized. :)");
    }
}
