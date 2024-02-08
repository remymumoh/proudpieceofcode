package org.code.category.kafka;

import com.google.protobuf.InvalidProtocolBufferException;
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
import java.util.Optional;

@Service
@Slf4j
@DependsOn("kafkaTopicsReady")
@RequiredArgsConstructor
public class CategoryConsumer {

   private final CategoryService categoryService;

    /**
     * Category Data product consumer
     *
     */
    @KafkaListener(clientIdPrefix = KafkaConstants.CATEGORY_PREFIX ,
        containerFactory = "kafkaProtobufManualACKListenerContainerFactory",
        autoStartup = "false",
        groupId = "catalog-svc",
        containerGroup = "g1",
        topicPartitions = @TopicPartition(
            topic = "${kafka.item-group-dp}",
            partitions = "#{@finder.partitions('${kafka.item-group-dp}')}",
            partitionOffsets = @PartitionOffset(partition = "*", initialOffset = "0")),
        properties = {
            "specific.protobuf.key.type: java.lang.String",
            "specific.protobuf.value.type: org.proudcode.setup.item_group.v1.ItemGroup"
        }
    )
    public void categoryRecordConsumer(ItemGroup itemGroup, Acknowledgment ack) {
        if (Objects.isNull(itemGroup) || ObjectUtils.isEmpty(itemGroup.getId())) {
            log.debug("Could not fetch category record proto from kafka: {}", itemGroup);
        } else {
            // Persist the  record to DB
            var categoryRecord = categoryService.saveCategoryRecord(org.code.merchandising.category.v1.ItemGroup.newBuilder()
                    .setId(itemGroup.getId())
                    .setName(itemGroup.getName())
                    .setParentItemGroupId(itemGroup.getParentItemGroupId())
                    .setDisabled(itemGroup.getDisabled())
                    .setDescription(itemGroup.getDescription())
                    .setImageUrl(itemGroup.getImageUrl())
                    .setKyoskType(String.valueOf(itemGroup.getKyoskType()))
                .build());
            log.trace("Saved category record for category:\n {}", categoryRecord);
        }
        ack.acknowledge();
    }

    private Optional<ItemGroup> getCategoryRecord(byte[] toByteArray) {
        try {
            var itemGroup = ItemGroup.parseFrom(toByteArray);
            log.trace("Received Category record : {}", itemGroup.getName());
            return Optional.of(itemGroup);
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        return Optional.empty();
    }
}
