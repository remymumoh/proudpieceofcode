package org.code.catalog.service.handlers;


import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class ProjectedQtyHandler {

    private final ProjectedQtyRepository projectedQtyRepository;
    private final CatalogProductRepository catalogProductRepository;
    private final CatalogPriceQtyRepository priceQtyRepository;

    public void processProjectedQty(String msgRef) {
        ProjectedQty projectedQty = projectedQtyRepository.findById(msgRef);
        List<Catalog> catList = catalogProductRepository.findByTerritoryIdAndStockItemId(projectedQty.getTerritoryId(), projectedQty.getItemId());
        catList.stream().map(catalog -> createOrUpdate(catalog, projectedQty))
            .map(this::computeSellableQty)
            .map(product-> product.toBuilder().setStockLastUpdated(CatalogUtils.getCurrentTime()).build())
            .forEach(priceQtyRepository::save);
        log.trace("PROJECTED_QTY CATALOG EVENT PROCESS FOR PB:{}", projectedQty.getId());

    }

    private CatalogProductPriceQty computeSellableQty(CatalogProductPriceQty.Builder builder) {

        Integer sellableQty = builder.getSellableStockItemsList()
            .stream()
            .map(StockItemSellableQty::getSellableQty)
            .min(Integer::compare)
            .get();

        return builder.setSellableQty(sellableQty).build();

    }


    private CatalogProductPriceQty.Builder createOrUpdate(Catalog catalog, ProjectedQty projectedQty) {
        Optional<CatalogProductPriceQty> optionalPriceQty = priceQtyRepository.findByCatalogProductId(catalog.getId());
        if (optionalPriceQty.isEmpty()) {
            log.error("No Projected Qty for {}", catalog.getId());
            return createPriceQty(catalog, projectedQty);
        }
        return updatePriceQty(catalog, optionalPriceQty.get(), projectedQty);
    }

    private CatalogProductPriceQty.Builder updatePriceQty(Catalog catalog, CatalogProductPriceQty priceQty, ProjectedQty projectedQty) {
        // create a sku -> CatalogStockItem
        Map<String, CatalogStockItem> catalogStockItemMap = catalog.getCatalogStockItemsList()
            .stream()
            .collect(Collectors.toMap(CatalogStockItem::getStockItemId, Function.identity()));

        // crate an sku -> SellableItemMap
        Map<String, StockItemSellableQty> sellableStockItemMap = priceQty.getSellableStockItemsList()
            .stream()
            .collect(Collectors.toMap(StockItemSellableQty::getStockItemId, Function.identity()));

        // iterate over  catalogStockItemMap
        List<StockItemSellableQty> newSellableItems = catalogStockItemMap.keySet()
            .stream()
            .map(stockItemId -> {
                // if catalogStockItem exists in SellableItemMap but is not equal to projected qty no need for update. return the same
                if (sellableStockItemMap.containsKey(stockItemId) && !Objects.equals(stockItemId, projectedQty.getItemId())) {
                    return sellableStockItemMap.get(stockItemId);
                }

                // Otherwise since catalogStockitem is source of truth create with 0 invetory
                return createSellableStockItem(catalogStockItemMap.get(stockItemId), projectedQty);
            })
            .toList();

        CatalogProductPriceQty.Builder toUpdatepriceQty = CatalogProductPriceQty.newBuilder(priceQty);
        toUpdatepriceQty.clearSellableStockItems();
        toUpdatepriceQty.addAllSellableStockItems(newSellableItems);
        return CatalogProductPriceQty.newBuilder(toUpdatepriceQty.build());
    }

    private CatalogProductPriceQty.Builder createPriceQty(Catalog catalog, ProjectedQty projectedQty) {
        List<StockItemSellableQty> sellableQtys = catalog.getCatalogStockItemsList()
            .stream()
            .map(catalogStockItem -> createSellableStockItem(catalogStockItem, projectedQty))
            .toList();

        return CatalogProductPriceQty.newBuilder()
            .setCatalogProductId(catalog.getId())
            .addAllSellableStockItems(sellableQtys);
    }

    private StockItemSellableQty createSellableStockItem(CatalogStockItem catalogStockItem, ProjectedQty projectedQty) {
        var builder = StockItemSellableQty.newBuilder()
            .setStockItemId(catalogStockItem.getStockItemId())
            .setConversionFactor(catalogStockItem.getConversionFactor())
            .setSellableQty(0);

        if (projectedQty != null && projectedQty.getItemId().equals(catalogStockItem.getStockItemId())) {
            builder.setSellableQty((int) (projectedQty.getProjectedQty() / catalogStockItem.getConversionFactor()));
        }
        return builder.build();
    }

}
