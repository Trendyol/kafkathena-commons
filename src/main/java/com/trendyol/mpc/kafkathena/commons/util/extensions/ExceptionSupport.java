package com.trendyol.mpc.kafkathena.commons.util.extensions;

import com.trendyol.mpc.kafkathena.commons.model.KSConsumer;
import com.trendyol.mpc.kafkathena.commons.model.exception.KSException;
import com.trendyol.mpc.kafkathena.commons.model.exception.KSIgnoredException;
import org.apache.commons.collections4.CollectionUtils;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Objects;

public interface ExceptionSupport {
    /**
     * Parse ignored exception classes that are defined in yaml configurations
     *
     * @param consumer
     * @param <T>
     * @return
     * @throws KSException
     */
    default <T> List<? extends Class<? extends Exception>> getIgnoredExceptionClasses(KSConsumer consumer) {//NOSONAR
        return (Objects.nonNull(consumer.getFailover()) && CollectionUtils.isNotEmpty(consumer.getFailover().getIgnoredExceptionClasses()))
                ? consumer.getFailover()
                .getIgnoredExceptionClasses()
                .stream()
                .map(item -> loadExceptionClass(consumer, item)).toList() : List.of();
    }

    @NotNull
    default Class<? extends Exception> loadExceptionClass(KSConsumer consumer, String item) {
        Class<?> aClass = null;
        Class<? extends Exception> exClass = null;
        try {
            aClass = Class.forName(item);
            exClass = aClass.asSubclass(Exception.class);
        } catch (Exception ex) {
            throw KSException.builder()
                    .message("Ignored Exception class not found for [" + item + "] being defined in kafkathena configuration. Consumer Factory Bean Name: " + consumer.getFactoryBeanName()).build();
        }
        return exClass;
    }

    @NotNull
    default Class<?> loadClass(String consumerName, String clazzPath) {
        try {
            return Class.forName(clazzPath);
        } catch (Exception ex) {
            throw KSException.builder()
                    .message("Class not found for [" + clazzPath + "] being defined in kafkathena configuration. Consumer: " + consumerName).build();
        }
    }

    default boolean isIgnoredException(KSConsumer consumer, Exception exception) {//NOSONAR It is required for generic exception definitions
        List<? extends Class<? extends Exception>> ignoredExceptionClasses = getIgnoredExceptionClasses(consumer);//NOSONAR It is required for generic exception definitions
        if (CollectionUtils.isNotEmpty(ignoredExceptionClasses)) {
            if (ignoredExceptionClasses.contains(exception.getClass())) {
                return true;
            } else {
                if (Objects.nonNull(exception.getCause())) {
                    Class<? extends Exception> causeClass = (Class<? extends Exception>) exception.getCause().getClass();
                    if (ignoredExceptionClasses.contains(causeClass)) {
                        return true;
                    }
                }
            }
        }
        return exception instanceof KSIgnoredException;
    }
}
