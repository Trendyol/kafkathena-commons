package com.trendyol.mpc.kafkathena.commons.model;

import lombok.Data;

@Data
public class KSFixedRetry {
    private Integer retryCount;
    private Long backoffIntervalMillis;
}
