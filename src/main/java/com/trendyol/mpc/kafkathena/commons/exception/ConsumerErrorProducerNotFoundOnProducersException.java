package com.trendyol.mpc.kafkathena.commons.exception;

import lombok.Builder;

public class ConsumerErrorProducerNotFoundOnProducersException extends RuntimeException{
    @Builder
    public ConsumerErrorProducerNotFoundOnProducersException(String errorProducerName) {
        super(String.format("Consumer error producer not found on producers. Error Producer Name: %s", errorProducerName));
    }
}
