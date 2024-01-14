package com.trendyol.mpc.kafkathena.commons.util.extensions;

import com.trendyol.mpc.kafkathena.commons.model.KSConsumerFactoryProp;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public interface KSMapSupport {
    default Map<String, Object> mergeKafkaProps(Map<String, Object> from, Map<String, Object> to) {
        if (from == null) {
            return to;
        }
        Map<Object, String> invertedFromMap = MapUtils.invertMap(from);
        return from.entrySet()
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
    }

    default KSConsumerFactoryProp mergeFactoryProps(KSConsumerFactoryProp defaultProps, KSConsumerFactoryProp targetProps) {
        KSConsumerFactoryProp ksDefaultProps = Optional.ofNullable(defaultProps).orElse(new KSConsumerFactoryProp());
        return Optional.ofNullable(targetProps)
                .map(tp -> {
                            KSConsumerFactoryProp ksConsumerFactoryProp = new KSConsumerFactoryProp();
                            ksConsumerFactoryProp.setSyncCommit(Optional.ofNullable(tp.getSyncCommit()).orElse(ksDefaultProps.getSyncCommit()));
                            ksConsumerFactoryProp.setSyncCommitTimeoutSecond(Optional.ofNullable(tp.getSyncCommitTimeoutSecond()).orElse(ksDefaultProps.getSyncCommitTimeoutSecond()));
                            ksConsumerFactoryProp.setAutoStartup(Optional.ofNullable(tp.getAutoStartup()).orElse(ksDefaultProps.getAutoStartup()));
                            ksConsumerFactoryProp.setConcurrency(Optional.ofNullable(tp.getConcurrency()).orElse(ksDefaultProps.getConcurrency()));
                            ksConsumerFactoryProp.setMissingTopicAlertEnable(Optional.ofNullable(tp.getMissingTopicAlertEnable()).orElse(ksDefaultProps.getMissingTopicAlertEnable()));
                            ksConsumerFactoryProp.setAckMode(Optional.ofNullable(tp.getAckMode()).orElse(ksDefaultProps.getAckMode()));
                            ksConsumerFactoryProp.setInterceptorClassPath(Optional.ofNullable(tp.getInterceptorClassPath()).orElse(ksDefaultProps.getInterceptorClassPath()));
                            return ksConsumerFactoryProp;
                        }
                )
                .orElse(ksDefaultProps);
    }
}