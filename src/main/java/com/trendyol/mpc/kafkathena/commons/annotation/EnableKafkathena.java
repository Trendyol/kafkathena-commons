package com.trendyol.mpc.kafkathena.commons.annotation;

import com.trendyol.mpc.kafkathena.commons.config.KSCommonConfiguration;
import org.springframework.context.annotation.Import;

import java.lang.annotation.*;

@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Import(KSCommonConfiguration.class)
public @interface EnableKafkathena {
}
