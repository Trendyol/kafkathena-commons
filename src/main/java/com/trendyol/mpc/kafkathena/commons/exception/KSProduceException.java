package com.trendyol.mpc.kafkathena.commons.exception;

import lombok.Builder;

public class KSProduceException extends RuntimeException {
    @Builder
    public KSProduceException(Throwable cause) {
        super("Erro sending message to kafka", cause);
    }
}