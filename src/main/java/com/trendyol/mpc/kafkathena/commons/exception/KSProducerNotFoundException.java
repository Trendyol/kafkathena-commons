package com.trendyol.mpc.kafkathena.commons.exception;

import lombok.Builder;

public class KSProducerNotFoundException extends RuntimeException {
    @Builder
    public KSProducerNotFoundException() {
        super("Any Producer is not defined on context");
    }
}
