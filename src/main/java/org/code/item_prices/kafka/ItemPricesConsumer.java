package org.code.item_prices.kafka;

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
import java.util.concurrent.atomic.AtomicInteger;

@Service
@Slf4j
@DependsOn("kafkaTopicsReady")
@RequiredArgsConstructor
public class ItemPricesConsumer {

    private final ItemPriceService itemPriceService;
    private AtomicInteger count = new AtomicInteger();
    private final ItemPriceHandler itemPriceHandler;


    /** ItemPrice
     * Inventory Item Data product consumer
     *
     */
    @KafkaListener(clientIdPrefix = KafkaConstants.ITEM_PRICE_PREFIX ,
        containerFactory = "kafkaProtobufManualACKListenerContainerFactory",
        autoStartup = "false",
        groupId = "catalog-svc",
        concurrency = "6",
        containerGroup = "g2",
        topicPartitions = @TopicPartition(
            topic = "${kafka.item-prices-dp}",
            partitions = "#{@finder.partitions('${kafka.item-prices-dp}')}",
            partitionOffsets = @PartitionOffset(partition = "*", initialOffset = "0")),
        properties = {
            "specific.protobuf.key.type: java.lang.String",
            "specific.protobuf.value.type: org.proudcode.stock.item_price.v1.ItemPrice"
        }
    )
    public void inventoryItemRecordConsumer(ItemPrice itemPrice, Acknowledgment ack)  {
        if (Objects.isNull(itemPrice) || ObjectUtils.isEmpty(itemPrice.getItemId())) {
            // do error handling
            log.debug("Could not fetch item price record proto from kafka: {}", itemPrice);
        }
        else {
            if (itemPrice.getSelling()) {
                // Persist the  record to DB
                itemPriceService.saveItemPriceRecord(itemPrice);
                itemPriceHandler.processItemPrice(itemPrice.getId());
                log.trace("Saved item record : {}", itemPrice.getName());
            }
        }
        if(count.incrementAndGet() % 1000 == 0 ){
            log.debug("total Item Prices processed {}",count.get());
        }
//        ack.acknowledge();
    }

    Optional<ItemPrice> getItemPriceRecord(byte[] toByteArray) {
        try {
            var itemPrice = ItemPrice.parseFrom(toByteArray);
            log.trace("itemPrice   record : {}", itemPrice.getItemId());
            return Optional.of(itemPrice);
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        return Optional.empty();
    }
}
