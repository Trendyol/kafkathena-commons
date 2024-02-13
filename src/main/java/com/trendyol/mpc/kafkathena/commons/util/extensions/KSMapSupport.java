package com.trendyol.mpc.kafkathena.commons.util.extensions;

import com.trendyol.mpc.kafkathena.commons.model.sharedfactory.KSSharedConsumerFactoryProperties;
import com.trendyol.mpc.kafkathena.commons.model.sharedfactory.KSSharedProducerFactoryProperties;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public interface KSMapSupport {
    Logger log = LoggerFactory.getLogger(KSMapSupport.class);

    default Map<String, Object> mergeKafkaProps(Map<String, Object> from, Map<String, Object> to) {
        if (from == null) {
            return to;
        }
        Map<Object, String> invertedFromMap = MapUtils.invertMap(from);
        HashMap<String, Object> merged = from.entrySet()
                .stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (v1, v2) -> {
                            if (invertedFromMap.get(v2).equalsIgnoreCase("spring.json.trusted.packages")) {
                                return StringUtils.join(Arrays.asList(StringUtils.deleteWhitespace(String.valueOf(v2)).trim(),
                                        StringUtils.deleteWhitespace(String.valueOf(v1)).trim()), ",");
                            } else {
                                return v2;
                            }
                        },
                        () -> new HashMap<>(to)));
        log.debug("Kafkathena: Merged props with default. {}", merged);
        return merged;
    }

    default KSSharedConsumerFactoryProperties mergeConsumerFactoryProps(KSSharedConsumerFactoryProperties defaultProps, KSSharedConsumerFactoryProperties targetProps) {
        KSSharedConsumerFactoryProperties ksDefaultProps = Optional.ofNullable(defaultProps).orElse(new KSSharedConsumerFactoryProperties());
        KSSharedConsumerFactoryProperties merged = Optional.ofNullable(targetProps)
                .map(tp -> {
                            KSSharedConsumerFactoryProperties ksSharedConsumerFactoryProperties = new KSSharedConsumerFactoryProperties();
                            ksSharedConsumerFactoryProperties.setSyncCommit(Optional.ofNullable(tp.getSyncCommit()).orElse(ksDefaultProps.getSyncCommit()));
                            ksSharedConsumerFactoryProperties.setSyncCommitTimeoutSecond(Optional.ofNullable(tp.getSyncCommitTimeoutSecond()).orElse(ksDefaultProps.getSyncCommitTimeoutSecond()));
                            ksSharedConsumerFactoryProperties.setAutoStartup(Optional.ofNullable(tp.getAutoStartup()).orElse(ksDefaultProps.getAutoStartup()));
                            ksSharedConsumerFactoryProperties.setConcurrency(Optional.ofNullable(tp.getConcurrency()).orElse(ksDefaultProps.getConcurrency()));
                            ksSharedConsumerFactoryProperties.setMissingTopicAlertEnable(Optional.ofNullable(tp.getMissingTopicAlertEnable()).orElse(ksDefaultProps.getMissingTopicAlertEnable()));
                            ksSharedConsumerFactoryProperties.setAckMode(Optional.ofNullable(tp.getAckMode()).orElse(ksDefaultProps.getAckMode()));
                            ksSharedConsumerFactoryProperties.setInterceptor(Optional.ofNullable(tp.getInterceptor()).orElse(ksDefaultProps.getInterceptor()));
                            ksSharedConsumerFactoryProperties.setBatch(Optional.ofNullable(tp.getBatch()).orElse(false));
                            return ksSharedConsumerFactoryProperties;
                        }
                )
                .orElse(ksDefaultProps);
        log.debug("Kafkathena: Merged sharedFactoryProps with consumer shared factory props. {}", merged);
        return merged;
    }

    default KSSharedProducerFactoryProperties mergeProducerFactoryProps(KSSharedProducerFactoryProperties defaultProps, KSSharedProducerFactoryProperties targetProps) {
        KSSharedProducerFactoryProperties ksDefaultProps = Optional.ofNullable(defaultProps).orElse(new KSSharedProducerFactoryProperties());
        KSSharedProducerFactoryProperties merged = Optional.ofNullable(targetProps)
                .map(tp -> {
                            KSSharedProducerFactoryProperties mergedProducerFactoryProps = new KSSharedProducerFactoryProperties();
                            mergedProducerFactoryProps.setInterceptor(Optional.ofNullable(tp.getInterceptor()).orElse(ksDefaultProps.getInterceptor()));
                            return mergedProducerFactoryProps;
                        }
                )
                .orElse(ksDefaultProps);
        log.debug("Kafkathena: Merged sharedFactoryProps with consumer shared factory props. {}", merged);
        return merged;
    }
}