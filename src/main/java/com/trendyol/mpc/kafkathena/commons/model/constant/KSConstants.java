package com.trendyol.mpc.kafkathena.commons.model.constant;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class KSConstants {
    public static final String KSCONSUMER_CONFIGURATION_BEAN_NAME = "kSConsumerConfiguration";
    public static final String KSPRODUCER_CONFIGURATION_BEAN_NAME = "kSProducerConfiguration";
    public static final String KS_BASE_PACKAGE = "com.trendyol.mpc.kafkathena";
    public static final String KS_DEFAULTS_YML_CLASSPATH = "classpath:kafkathena-defaults.yml";
    public static final String KS_CONFIG_PREFIX = "kafkathena";
}
