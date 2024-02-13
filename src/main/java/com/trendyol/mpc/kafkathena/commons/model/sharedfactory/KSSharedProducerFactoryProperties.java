package com.trendyol.mpc.kafkathena.commons.model.sharedfactory;

import lombok.Data;

@Data
public class KSSharedProducerFactoryProperties {
    private String interceptor = "com.trendyol.mpc.kafkathena.commons.interceptor.KSProducerInterceptor";
}
