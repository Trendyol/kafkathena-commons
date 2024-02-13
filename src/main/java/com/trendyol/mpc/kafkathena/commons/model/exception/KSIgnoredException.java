package com.trendyol.mpc.kafkathena.commons.model.exception;

import lombok.Builder;

public class KSIgnoredException extends RuntimeException {
    @Builder
    public KSIgnoredException(String message) {
        super(message);
    }
}
