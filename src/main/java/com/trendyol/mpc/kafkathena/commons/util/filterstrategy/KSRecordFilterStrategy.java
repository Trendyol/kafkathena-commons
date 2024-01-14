package com.trendyol.mpc.kafkathena.commons.util.filterstrategy;

import com.trendyol.mpc.kafkathena.commons.model.constant.KSHeader;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.listener.adapter.RecordFilterStrategy;

public class KSRecordFilterStrategy implements RecordFilterStrategy<String, Object> {
    private final String filterKey;

    public KSRecordFilterStrategy(String filterKey) {
        this.filterKey = filterKey;
    }

    @Override
    public boolean filter(ConsumerRecord<String, Object> rec) {
        Header filterTextHeader = rec.headers().lastHeader(KSHeader.FILTER_KEY.getHeaderKeyName());
        return filterTextHeader == null || !new String(filterTextHeader.value()).equals(filterKey);
    }
}