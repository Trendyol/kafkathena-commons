package com.trendyol.mpc.kafkathena.commons.util.filter;

import com.trendyol.mpc.kafkathena.commons.model.constant.KSHeader;
import com.trendyol.mpc.kafkathena.commons.util.filterstrategy.KSRecordFilterStrategy;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
class KSRecordFilterStrategyTest {
    @Test
    public void it_should_filter_incoming_filter_value_equals_to_defined_filter_value() {
        //given
        KSRecordFilterStrategy ksRecordFilterStrategy = new KSRecordFilterStrategy("blah");
        ConsumerRecord record = new ConsumerRecord("topic", -1, -1, null, null);
        record.headers().add(KSHeader.FILTER_KEY.getHeaderKeyName(), "blah".getBytes(StandardCharsets.UTF_8));
        //when
        boolean result = ksRecordFilterStrategy.filter(record);
        //then
        assertThat(result).isFalse();

    }

    @Test
    public void it_should_not_filter_incoming_filter_value_not_equals_to_defined_filter_value() {
        //given
        KSRecordFilterStrategy ksRecordFilterStrategy = new KSRecordFilterStrategy("blah1");
        ConsumerRecord record = new ConsumerRecord("topic", -1, -1, null, null);
        record.headers().add(KSHeader.FILTER_KEY.getHeaderKeyName(), "blah".getBytes(StandardCharsets.UTF_8));
        //when
        boolean result = ksRecordFilterStrategy.filter(record);
        //then
        assertThat(result).isTrue();

    }

    @Test
    public void it_should_not_filter_incoming_filter_value_is_empty() {
        //given
        KSRecordFilterStrategy ksRecordFilterStrategy = new KSRecordFilterStrategy("blah1");
        ConsumerRecord record = new ConsumerRecord("topic", -1, -1, null, null);
        //when
        boolean result = ksRecordFilterStrategy.filter(record);
        //then
        assertThat(result).isTrue();

    }
}