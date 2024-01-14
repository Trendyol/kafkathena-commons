package com.trendyol.mpc.kafkathena.commons.model;

import lombok.Data;

import java.util.List;

@Data
public class KSConsumerFailover {
    private String handlerBeanName;
    private String errorTopic;
    private List<String> ignoredExceptionClasses;
}
