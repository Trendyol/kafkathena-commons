package com.trendyol.mpc.kafkathena.commons.model;

import lombok.Data;

@Data
public class KSFilterHeader {
   private String consumerFilterKey;
   private String errorProducerFilterKey;
}
