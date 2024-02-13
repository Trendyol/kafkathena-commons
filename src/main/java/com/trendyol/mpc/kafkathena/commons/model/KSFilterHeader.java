package com.trendyol.mpc.kafkathena.commons.model;

import lombok.Data;

import java.util.List;

@Data
public class KSFilterHeader {
    private String consumerFilterKey;
    private String errorProducerFilterKey;
    private List<CustomKsFilterHeader> customHeaders;

    public record CustomKsFilterHeader(String key, String value) {
    }
}
