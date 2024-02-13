package com.trendyol.mpc.kafkathena.commons.model.exception;

import lombok.Builder;

public class KSException extends RuntimeException {
    @Builder
    public KSException(String message) {
        super(message);
    }

    @Builder
    public KSException(String message, Throwable cause) {
        super(message, cause);
    }

    @Override
    public String getMessage() {
        return "Kafkathena: ".concat(super.getMessage());
    }
}