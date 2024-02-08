package org.code.catalog.service.handlers;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class ProductBundleHandler {

    private final ProductBundleRepository productBundleRepository;
    private final CatalogProductRepository catalogProductRepository;
    private final CatalogIndex searchIndex;
    private final CatalogProducer catalogProducer;

    /**
     * I only use the product bundle to update the status or the description of an already existing Catalog.
     */
    public void processProductBundle(String msgRef) {
        log.info("PRODUCT_BUNDLE CATALOG EVENT PROCESS Started{} FOR PB:{}", Instant.now(), msgRef);

        ProductBundle productBundle = productBundleRepository.findByNonStockItemId(msgRef);

        if (Objects.isNull(productBundle)) {
            return;
        }

        Map<String, Boolean> territoryStatusMap = CatalogUtil.getTerritoryStatusMap(productBundle);

        territoryStatusMap.keySet()
            .stream()
            .map(territoryId -> createCatalog(productBundle, territoryId, territoryStatusMap))
            .forEach(catalog -> {
                Catalog savedCatalog = catalogProductRepository.save(catalog);
                catalogProducer.publishCatalogDP(savedCatalog);
            });
        String searchString = "%s %s".formatted(productBundle.getNonStockItemId().toLowerCase(), productBundle.getItemGroupId().toLowerCase());
        log.trace("PRODUCT_BUNDLE CATALOG EVENT PROCESS FOR PB:{} for searchstring:{}", productBundle.getId(), searchString);
        searchIndex.addToIndex(searchString, productBundle.getId());
    }


    private Catalog createCatalog(ProductBundle productBundle, String territoryId, Map<String, Boolean> territoryStatusMap) {
        List<CatalogStockItem> itemList = productBundle.getStockItemsList()
            .stream()
            .map(this::getCatalogStockItem)
            .collect(Collectors.toList());

        //        TODO: Publish catalog to kafka

        return Catalog.newBuilder()
            .setId(CatalogUtil.getCatalogId(territoryId, productBundle.getNonStockItemId()))
            .setTerritoryId(territoryId)
            .setItemName(productBundle.getNonStockItemId())
            .setImageUrl(productBundle.getImageUrl())
            .setItemDescription(productBundle.getDescription())
            .setNonStockItemId(productBundle.getNonStockItemId())
            .setCategoryId(productBundle.getItemGroupId())
            .setDisabled(CatalogUtil.isDisabled(productBundle, territoryId, territoryStatusMap))
            .setKyoskProductBundleId(productBundle.getId())
            .setUom(productBundle.getUom())
            .addAllCatalogStockItems(itemList)
            .build();
    }

    private CatalogStockItem getCatalogStockItem(StockItem stockItem) {
        return CatalogStockItem.newBuilder()
            .setStockItemId(stockItem.getStockItemId())
            .setConversionFactor(stockItem.getConversionFactor())
            .setStockUom(stockItem.getStockUom())
            .setDimension(stockItem.getDimension())
            .build();
    }

}
