package com.trendyol.mpc.kafkathena.commons.util.extensions;

import org.jetbrains.annotations.NotNull;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

@Component
public class SpringContext implements ApplicationContextAware {
    public static ApplicationContext context;//NOSONAR

    @Override
    public void setApplicationContext(@NotNull ApplicationContext context) throws BeansException {
        SpringContext.context = context;//NOSONAR
    }
}
