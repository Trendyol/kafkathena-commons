package com.trendyol.mpc.kafkathena.commons.model;

import lombok.Data;

import java.util.Map;

@Data
public class KSProducer {
    private Map<String, Object> props;
}
