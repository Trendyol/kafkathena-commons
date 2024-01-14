package com.trendyol.mpc.kafkathena.commons.util;

import com.trendyol.mpc.kafkathena.commons.exception.KSProducerNotFoundException;
import com.trendyol.mpc.kafkathena.commons.model.KSConsumerProducerProp;
import com.trendyol.mpc.kafkathena.commons.util.extensions.KSMapSupport;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.commons.collections4.MapUtils;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Getter
@Component
@RequiredArgsConstructor
public class KSProducerHelper implements KSMapSupport {
    private final KSConsumerProducerProp ksConsumerProducerProp;
    private final ConcurrentHashMap<String, KafkaTemplate<String, Object>> kafkathenaProducerMap = new ConcurrentHashMap<>();

    public ConcurrentHashMap<String, KafkaTemplate<String, Object>> getProducerMap() {
        return getKafkathenaProducerMap();
    }

    public KafkaTemplate<String, Object> getProducer(String name) {
        return getProducerMap()
                .get(name);
    }

    public void initProducers() {
        if (MapUtils.isEmpty(ksConsumerProducerProp.getProducers())) {
            throw new KSProducerNotFoundException();
        }

        Optional.ofNullable(ksConsumerProducerProp.getProducers())
                .filter(MapUtils::isNotEmpty)
                .ifPresent(producers -> {
                    producers
                            .forEach((name, props) -> {
                                DefaultKafkaProducerFactory<String, Object> kafkaProducerFactory =
                                        new DefaultKafkaProducerFactory<>(mergeKafkaProps(props.getProps(), ksConsumerProducerProp.getProducerPropsDefaults()));
                                kafkathenaProducerMap.putIfAbsent(name.concat("_kt_producer"), new KafkaTemplate<>(kafkaProducerFactory));
                            });
                });
    }
}
