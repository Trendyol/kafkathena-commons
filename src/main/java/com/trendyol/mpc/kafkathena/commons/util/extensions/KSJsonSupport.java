package com.trendyol.mpc.kafkathena.commons.util.extensions;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trendyol.mpc.kafkathena.commons.model.exception.KSException;

public interface KSJsonSupport {
    default <T> String asJson(ObjectMapper objectMapper, T in) {
        try {
            return objectMapper.writeValueAsString(in);
        } catch (JsonProcessingException j) {
            throw new KSException(String.format("Json Convert has an error for payload: %s", in));
        }
    }
}