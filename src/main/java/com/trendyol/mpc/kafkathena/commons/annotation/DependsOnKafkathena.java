package com.trendyol.mpc.kafkathena.commons.annotation;

import com.trendyol.mpc.kafkathena.commons.model.constant.KSConstants;
import org.springframework.context.annotation.DependsOn;

import java.lang.annotation.*;

@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@DependsOn(KSConstants.KSCONSUMER_CONFIGURATION_BEAN_NAME)
public @interface DependsOnKafkathena {
}
