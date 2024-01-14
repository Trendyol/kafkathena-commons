package com.trendyol.mpc.kafkathena.commons.model;

import lombok.AccessLevel;
import lombok.Data;
import lombok.Setter;

import java.util.Map;

@Data
public class KSConsumer {
    private String type;
    private String topic;
    private Map<String, Object> props;
    private String errorProducerName;
    private String factoryBeanName;
    private String dataClass;
    private KSFixedRetry fixedRetry;
    private KSExponentialRetry exponentialRetry;
    private KSConsumerFactoryProp factoryProps;
    private KSConsumerFailover failover;
    private KSFilterHeader filterHeader;
    @Setter(AccessLevel.NONE)
    private Object valueDeserializer;
    @Setter(AccessLevel.NONE)
    private Object keyDeserializer;
}