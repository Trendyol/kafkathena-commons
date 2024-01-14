package com.trendyol.mpc.kafkathena.commons.util;

import com.trendyol.mpc.kafkathena.commons.model.KSConsumerProducerProp;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
@RequiredArgsConstructor
@Slf4j
public class KSConsumerHelper {
    private final KSKafkaUtil ksKafkaUtil;
    private final KSConsumerProducerProp ksConsumerProducerProp;
    public void initConsumers(){
        Optional.ofNullable(ksConsumerProducerProp.getConsumers())
                .filter(MapUtils::isNotEmpty)
                .ifPresent(consumers -> consumers.forEach((consumerName, consumerInfo) -> {
                    ksKafkaUtil.createAndPublishListenerFactory(consumerInfo);
                    log.info("Kafkathena consumer bindings initialized {}", ksConsumerProducerProp);
                }));
    }
}
