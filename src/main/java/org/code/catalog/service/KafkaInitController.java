package org.code.catalog.service;


import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.event.ListenerContainerIdleEvent;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class KafkaInitController {

    private final KafkaListenerEndpointRegistry registry;
    private final ApplicationContext ctx;
    private final ApplicationEventPublisher publisher;

    private final ProductBundleRepository pbRepo;
    private final CategoryRepository categoryRepository;
    private final TerritoryRepository territoryRepository;
    private final ItemPricesRepository itemPricesRepository;
    private final ProjectedQtyRepository projectedQtyRepository;

    private KafkaInMemoryCacheLoadSequencer cacheLoadSequencer;

    private final CatalogQueueProcessor queueProcessor;
    private final PartitionFinder finder;
    private boolean isLoaded;



    @EventListener(ApplicationReadyEvent.class)
    public void startApp() {
        this.cacheLoadSequencer = getCacheLoadSequencer();
        cacheLoadSequencer.initialize();
        cacheLoadSequencer.start();
    }

    @EventListener(ListenerContainerIdleEvent.class)
    protected void idleEvents(ListenerContainerIdleEvent evt){
        if (!isLoaded) {
            this.cacheLoadSequencer.onApplicationEvent(evt);
        }
    }

    @EventListener(KafkaInMemoryCacheEvent.class)
    public void startIndexCreation(KafkaInMemoryCacheEvent event){
        // if the cache has been populated process the stream and resume topic consumption
        if (event.getCacheStatus().equals(KafkaInMemoryCacheEvent.CacheStatus.DataLoaded)) {
            log.info("""
                Init Cache Stats
                Product Bundles Loaded {}
                territory Loaded {}
                Category Loaded {}
                Item Prices {}
                Projected Quantities {}
                Item Prices with Missing price list {}
                Item Prices with zero price {}
                """,
                pbRepo.getSize(),territoryRepository.getSize(),
                categoryRepository.getSize(),itemPricesRepository.getSize(),
                projectedQtyRepository.getSize(),
                itemPricesRepository.getNoPriceListSize(),
                itemPricesRepository.getZeroPriceSize()
            );
            queueProcessor.consumeEventStream();
            cacheLoadSequencer.resume();
            this.isLoaded = true;
            finder.closeConsumer();
        }
    }

    /**
     * I want to load the categories and the product bundles first. The product bundles will only be stored in cache but will not trigger the creation of item prices
     * The Product bundles provide us with status of the
            g1 -> Categories, Territories & Product Bundles
            g2 -> Item Prices & Projected Quantities
    */
    KafkaInMemoryCacheLoadSequencer getCacheLoadSequencer() {
        var sequencer = new KafkaInMemoryCacheLoadSequencer(registry, publisher, ctx, 2000,"g1", "g2");
        return sequencer;
    }
}
