package org.code.adevsetup;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

@Configuration
@RequiredArgsConstructor
@Slf4j
public class KafkaTopics {

    @Value("${kafka.product-bundle-dp}")
    private String productBundleTopic;

    @Value("${kafka.item-prices-dp}")
    private String itemPricesTopic;

    @Value("${kafka.item-group-dp}")
    private String itemGroupTopic;

    @Value("${kafka.projected-qty-dp}")
    private String projectedQtyTopic;

    private final KafkaAdmin kafkaAdmin;

    private final Environment env;


    @Bean
    protected Boolean kafkaTopicsReady(){
        if(env.acceptsProfiles(Profiles.of("dev","vcluster"))){
            createTopics();
        }
        return true;
    }

    protected void  createTopics(){
        kafkaAdmin.createOrModifyTopics(
            TopicBuilder.name(productBundleTopic).partitions(3).build(),
            TopicBuilder.name(itemPricesTopic).build(),
            TopicBuilder.name(itemGroupTopic).build(),
            TopicBuilder.name(projectedQtyTopic).build()
        );
    }

}
