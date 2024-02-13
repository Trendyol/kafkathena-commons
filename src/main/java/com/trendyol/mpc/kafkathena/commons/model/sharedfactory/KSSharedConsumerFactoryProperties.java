package com.trendyol.mpc.kafkathena.commons.model.sharedfactory;

import lombok.*;
import org.springframework.kafka.listener.ContainerProperties;

@Getter @Setter @NoArgsConstructor @AllArgsConstructor
public class KSSharedConsumerFactoryProperties {
    private String interceptor = "com.trendyol.mpc.kafkathena.commons.interceptor.KSConsumerInterceptor";
    private Boolean autoStartup = true;
    private Boolean missingTopicAlertEnable = false;
    private Integer concurrency = 1;
    private Integer syncCommitTimeoutSecond = 5;
    private Boolean syncCommit = true;
    private Boolean batch = false;
    private ContainerProperties.AckMode ackMode = ContainerProperties.AckMode.RECORD;

    @Override
    public String toString() {
        return "  interceptor='" + interceptor + '\n' +
               "  autoStartup=" + autoStartup + '\n' +
               "  missingTopicAlertEnable=" + missingTopicAlertEnable + '\n' +
               "  concurrency=" + concurrency + '\n' +
               "  syncCommitTimeoutSecond=" + syncCommitTimeoutSecond + '\n' +
               "  syncCommit=" + syncCommit + '\n' +
               "  batch=" + batch + '\n' +
               "  ackMode=" + ackMode;
    }
}
