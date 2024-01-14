package com.trendyol.mpc.kafkathena.commons.model;

import lombok.Data;

@Data
public class KSExponentialRetry {
    private Integer retryCount;
    private Double multiplier;
    private Long maxInterval;
    private Long backoffIntervalMillis;
}
