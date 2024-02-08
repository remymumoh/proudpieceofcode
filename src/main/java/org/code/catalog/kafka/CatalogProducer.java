package org.code.catalog.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class CatalogProducer {
    @Value("${kafka.producers.catalog}")
    private String topic;

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Async
    public void publishCatalogDP(Catalog catalog) {
         if (ObjectUtils.isNotEmpty(catalog.getId()))
            kafkaTemplate.send(topic, catalog.getId(), catalog);
    }
}
