package org.code.catalog.service;


import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.Disposable;

import java.time.Instant;

@Service
@RequiredArgsConstructor
@Slf4j
public class CatalogQueueProcessor {

    private final CatalogQueueService queueService;
    private final ProductBundleHandler productBundleHandler;
    private final ItemPriceHandler itemPriceHandler;
    private final ProjectedQtyHandler projectedQtyHandler;
    private final CatalogProductRepository catalogProductRepository;

    private Disposable subscription;


    public void consumeEventStream() {
        this.subscription = queueService.getEventStream().subscribe(this::getMsgEventConsumer);
        catalogProductRepository.getSize();
    }

    private void getMsgEventConsumer(MsgEvent msgEvent) {
        try {
            log.info("Event Type:{} message Ref:{} event time:{}", msgEvent.getType(), msgEvent.getMsgRef(), Instant.now());
            // I have everything in a try catch so that I do not kill the queue
            switch (msgEvent.getType()) {
                case PRODUCT_BUNDLE -> productBundleHandler.processProductBundle(msgEvent.getMsgRef());
                case ITEM_PRICE -> itemPriceHandler.processItemPrice(msgEvent.getMsgRef());
                case PROJECTED_QTY -> projectedQtyHandler.processProjectedQty(msgEvent.getMsgRef());
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.debug("Issue processing message {}", msgEvent);
            log.error("error {}", e.getMessage());
        }
    }
}
