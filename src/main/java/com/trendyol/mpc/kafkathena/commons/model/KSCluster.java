package com.trendyol.mpc.kafkathena.commons.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class KSCluster {
    private String servers;
    private Map<String, Object> additionalProps;
}
