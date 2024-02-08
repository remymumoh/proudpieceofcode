package org.code.config.kafka;

import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;

@Configuration
@Slf4j
public class KafkaProducersConfig {


    @Autowired
    private ProducerFactory<String, ?> kafkaProducerFactory;


    @Bean
    public KafkaTemplate<String, Object> protoTemplate() {
        var configs = new HashMap<>(kafkaProducerFactory.getConfigurationProperties());
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class);
        log.debug("configs {}", configs);
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(configs));
    }


}
