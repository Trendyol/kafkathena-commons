package com.trendyol.mpc.kafkathena.commons.model;

import lombok.Data;
import org.springframework.kafka.listener.ContainerProperties;

@Data
public class KSConsumerFactoryProp {
    private String interceptorClassPath;
    private Boolean autoStartup = true;
    private Boolean missingTopicAlertEnable = false;
    private Integer concurrency = 1;
    private Integer syncCommitTimeoutSecond = 5;
    private Boolean syncCommit = true;
    private ContainerProperties.AckMode ackMode = ContainerProperties.AckMode.RECORD;
}
