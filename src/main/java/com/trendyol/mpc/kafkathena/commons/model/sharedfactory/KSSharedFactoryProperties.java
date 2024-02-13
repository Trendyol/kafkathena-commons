package com.trendyol.mpc.kafkathena.commons.model.sharedfactory;

import com.trendyol.mpc.kafkathena.commons.model.KSCluster;
import lombok.Data;

import java.util.Map;

@Data
public class KSSharedFactoryProperties {
    private KSSharedProducerFactoryProperties producer;
    private KSSharedConsumerFactoryProperties consumer;
    private Map<String, KSCluster> clusters;
}
