package com.trendyol.mpc.kafkathena.commons.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.EncodedResource;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
class KSYamlPropertySourceFactoryTest {
    private KSYamlPropertySourceFactory ksYamlPropertySourceFactory = new KSYamlPropertySourceFactory();
    @Test
    public void it_should_CreatePropertySource() {
        //given
        Resource res = new ClassPathResource("kafkathena-defaults.yml");
        EncodedResource encRes = new EncodedResource(res, "UTF-8");

        //when
        PropertySource<?> blah = ksYamlPropertySourceFactory.createPropertySource("blah", encRes);
        //then
        assertThat(blah.getProperty("kafkathena.consumer-props-defaults[max.poll.interval.ms]")).isEqualTo("${EVENT_CONSUMER_DEFAULTS_MAX_POLL_INTERVAL_MS:300000}");

    }
}