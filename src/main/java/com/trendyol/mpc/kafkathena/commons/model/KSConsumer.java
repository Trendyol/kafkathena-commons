package com.trendyol.mpc.kafkathena.commons.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.trendyol.mpc.kafkathena.commons.model.sharedfactory.KSSharedConsumerFactoryProperties;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Setter;

import java.util.Map;

@Data
public class KSConsumer {
    @JsonIgnore
    private String name;
    private String type;
    private String topic;
    private String cluster;
    private String errorProducerName;
    private String factoryBeanName;
    private String dataClass;
    private Boolean isBatch;
    private KSFixedRetry fixedRetry;
    private KSExponentialRetry exponentialRetry;
    private KSSharedConsumerFactoryProperties factoryProps;
    private KSConsumerFailover failover;
    private KSFilterHeader filterHeader;
    private Map<String, Object> props;
    @Setter(AccessLevel.NONE)
    private Object valueDeserializer;
    @Setter(AccessLevel.NONE)
    private Object keyDeserializer;
}