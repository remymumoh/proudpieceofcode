package org.code.product_bundles.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.DependsOn;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;

import java.util.Objects;

@Service
@Slf4j
@DependsOn("kafkaTopicsReady")
@RequiredArgsConstructor
public class ProductBundleConsumer {

    private final ProductService productService;
    private final ProductBundleHandler productBundleHandler;

    @KafkaListener(clientIdPrefix = KafkaConstants.BUNDLE_PREFIX,
        containerFactory = "kafkaProtobufManualACKListenerContainerFactory",
        autoStartup = "false",
        groupId = "catalog-svc",
        containerGroup = "g1",
        topicPartitions = @TopicPartition(
            topic = "${kafka.product-bundle-dp}",
            partitions = "#{@finder.partitions('${kafka.product-bundle-dp}')}",
            partitionOffsets = @PartitionOffset(partition = "*", initialOffset = "0")),
        properties = {
            "specific.protobuf.key.type: java.lang.String",
            "specific.protobuf.value.type: org.code.selling.product_bundle.v1.ProductBundle"
        }
    )
    public void productRecordConsumer(ProductBundle productBundle, Acknowledgment ack)  {
        if (Objects.isNull(productBundle) || ObjectUtils.isEmpty(productBundle.getId())) {
            log.debug("Could not fetch product bundle record proto from kafka: {}", productBundle);
        } else {
            productService.saveProductBundleRecord(productBundle);
            productBundleHandler.processProductBundle(productBundle.getId());
        }
    }
}
