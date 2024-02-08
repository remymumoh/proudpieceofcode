package org.code.projected_qty.kafka;


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
import java.util.concurrent.atomic.AtomicInteger;

@Service
@Slf4j
@DependsOn("kafkaTopicsReady")
@RequiredArgsConstructor
public class ProjectedQtyConsumer {

    private final ProjectedQtyService projectedQtyService;
    private AtomicInteger count = new AtomicInteger();
    private final ProjectedQtyHandler projectedQtyHandler;

    /**
     * ProjectedQty Data product consumer
     */
    @KafkaListener(clientIdPrefix = KafkaConstants.PROJECTED_QTY_PREFIX,
        containerFactory = "kafkaProtobufManualACKListenerContainerFactory",
        autoStartup = "false",
        groupId = "catalog-svc",
        containerGroup = "g2",
        concurrency = "3",
        topicPartitions = @TopicPartition(
            topic = "${kafka.projected-qty-dp}",
            partitions = "#{@finder.partitions('${kafka.projected-qty-dp}')}",
            partitionOffsets = @PartitionOffset(partition = "*", initialOffset = "0")),
        properties = {
            "specific.protobuf.key.type: java.lang.String",
            "specific.protobuf.value.type: org.proudcode.merchandising.projected_qty.v1.ProjectedQty"
        }
    )

    public void projectedQtyRecordConsumer(ProjectedQty projectedQty, Acknowledgment ack) {
        if (Objects.isNull(projectedQty) || ObjectUtils.isEmpty(projectedQty.getId())) {
            log.debug("Could not fetch projectedQty record proto from kafka: {}", projectedQty);
        } else {
            projectedQtyService.saveProjectedQtyRecord(projectedQty);
            projectedQtyHandler.processProjectedQty(projectedQty.getId());
            log.trace("Saved projectedQty record: {}", projectedQty);
        }
        if (count.incrementAndGet() % 1000 == 0) {
            log.debug("total Projected Qty processed {}", count.get());
        }
    }


}
