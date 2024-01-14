package com.trendyol.mpc.kafkathena.commons.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class KSDeserializerConfig {
    private Object keyDeserializer;
    private Object valueDeserializer;
}
