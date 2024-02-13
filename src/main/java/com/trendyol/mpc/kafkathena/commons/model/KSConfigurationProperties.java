package com.trendyol.mpc.kafkathena.commons.model;

import com.trendyol.mpc.kafkathena.commons.model.constant.KSConstants;
import com.trendyol.mpc.kafkathena.commons.model.sharedfactory.KSSharedFactoryProperties;
import com.trendyol.mpc.kafkathena.commons.util.extensions.KSYamlPropertySourceFactory;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
@ConfigurationProperties(prefix = KSConstants.KS_CONFIG_PREFIX)
@Data
@RequiredArgsConstructor
@ToString
@PropertySource(value = KSConstants.KS_DEFAULTS_YML_CLASSPATH, factory = KSYamlPropertySourceFactory.class)
public class KSConfigurationProperties {
    private KSSharedFactoryProperties sharedFactoryProps;
    private Map<String, KSConsumer> consumers;
    private Map<String, KSProducer> producers;
    private Map<String, String> integrationTopics;
    private Map<String, Object> consumerPropsDefaults;
    private Map<String, Object> producerPropsDefaults;
}