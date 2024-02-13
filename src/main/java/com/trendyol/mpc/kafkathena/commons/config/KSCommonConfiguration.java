package com.trendyol.mpc.kafkathena.commons.config;

import com.trendyol.mpc.kafkathena.commons.model.constant.KSConstants;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.retry.annotation.EnableRetry;


@EnableKafka
@EnableRetry
@ComponentScan(basePackages = {KSConstants.KS_BASE_PACKAGE})
@AutoConfiguration
public class KSCommonConfiguration {

}
