package org.code.territory.kafka;

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
public class TerritoryConsumer {

    private final TerritoryRepository territoryRepository;

    @KafkaListener(clientIdPrefix = KafkaConstants.TERRITORY_PREFIX,
        containerFactory = "kafkaProtobufManualACKListenerContainerFactory",
        autoStartup = "false",
        groupId = "catalog-svc",
        containerGroup = "g1",
        topicPartitions = @TopicPartition(
            topic = "${kafka.territory-dp}",
            partitions = "#{@finder.partitions('${kafka.territory-dp}')}",
            partitionOffsets = @PartitionOffset(partition = "*", initialOffset = "0")),
        properties = {
            "specific.protobuf.key.type: java.lang.String",
            "specific.protobuf.value.type: org.code.setup.territory.v1.Territory"
        }
    )
    public void territoryRecordConsumer(Territory territory, Acknowledgment ack) {
        try {
            if (Objects.isNull(territory) || ObjectUtils.isEmpty(territory.getId())) {
                log.debug("Could not fetch Territory record proto from kafka: {}", territory);
            } else {
                // Persist the  record to DB
                if (AppConstants.MARKET.equals(territory.getTerritoryType())) {
                    var persistedTerritoryRecord = territoryRepository.save(territory);
                    log.trace("Saved Territory record for category: {}", persistedTerritoryRecord.getId());
                }
            }
        } catch (Exception ex) {
            log.error("territoryRecordConsumer Exception {}", ex.getMessage());
        }
        ack.acknowledge();
    }
}
