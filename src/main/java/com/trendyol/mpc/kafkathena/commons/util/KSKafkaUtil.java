package com.trendyol.mpc.kafkathena.commons.util;

import com.trendyol.mpc.kafkathena.commons.exception.KSException;
import com.trendyol.mpc.kafkathena.commons.model.KSConsumer;
import com.trendyol.mpc.kafkathena.commons.model.KSConsumerFactoryProp;
import com.trendyol.mpc.kafkathena.commons.model.KSConsumerProducerProp;
import com.trendyol.mpc.kafkathena.commons.util.extensions.KSJsonSupport;
import com.trendyol.mpc.kafkathena.commons.util.extensions.KSMapSupport;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
@EnableRetry
@RequiredArgsConstructor
@Log4j2
public class KSKafkaUtil implements KSMapSupport, KSJsonSupport {
    private final KSKafkaHelper ksKafkaHelper;
    private final KSConsumerProducerProp ksConsumerProducerProp;
    private final ApplicationContext applicationContext;

    @SneakyThrows
    public void createAndPublishListenerFactory(KSConsumer consumer) {
        validateConsumerRequiredProps(consumer);
        Class<?> clz = Class.forName(consumer.getDataClass());
        KSConsumerFactoryProp ksConsumerFactoryProp = mergeFactoryProps(ksConsumerProducerProp.getSharedFactoryProps(), consumer.getFactoryProps());
        ConsumerFactory<String, ?> consumerFactory = ksKafkaHelper.createSpringConsumerFactory(consumer, clz, ksConsumerFactoryProp);
        ConcurrentKafkaListenerContainerFactory<String, ?> kafkaSingleListenerContainerFactory = ksKafkaHelper.createSpringKafkaListenerContainerFactory(consumerFactory, consumer, ksConsumerFactoryProp);
        ConfigurableListableBeanFactory beanFactory = ((ConfigurableApplicationContext) applicationContext).getBeanFactory();
        beanFactory.registerSingleton(consumer.getFactoryBeanName(), kafkaSingleListenerContainerFactory);
    }

    public void validateConsumerRequiredProps(KSConsumer consumer){
        Map<String, Object> consumerProps = mergeKafkaProps(consumer.getProps(), ksConsumerProducerProp.getConsumerPropsDefaults());
        if(!consumerProps.containsKey("group.id")){
            throw new KSException(String.format("Consumer Factory [%s] has not [group.id]", consumer.getFactoryBeanName()));
        }
        if(!consumerProps.containsKey("value.deserializer")){
            throw new KSException(String.format("Consumer Factory [%s] has not [value.deserializer]", consumer.getFactoryBeanName()));
        }
        if(!consumerProps.containsKey("spring.deserializer.value.delegate.class")){
            throw new KSException(String.format("Consumer Factory [%s] has not [spring.deserializer.value.delegate.class]", consumer.getFactoryBeanName()));
        }
        if(!consumerProps.containsKey("key.deserializer")){
            throw new KSException(String.format("Consumer Factory [%s] has not [key.deserializer]", consumer.getFactoryBeanName()));
        }
        if(!consumerProps.containsKey("spring.deserializer.key.delegate.class")){
            throw new KSException(String.format("Consumer Factory [%s] has not [spring.deserializer.key.delegate.class]", consumer.getFactoryBeanName()));
        }

    }

}
