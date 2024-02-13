package com.trendyol.mpc.kafkathena.commons.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.trendyol.mpc.kafkathena.commons.model.sharedfactory.KSSharedProducerFactoryProperties;
import lombok.Data;

import java.util.Map;

@Data
public class KSProducer {
    @JsonIgnore
    private String name;
    private String cluster;
    private Map<String, Object> props;
    private KSSharedProducerFactoryProperties factoryProps;
}
