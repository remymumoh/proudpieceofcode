package org.proudcode;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Repository;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Slf4j
@RequiredArgsConstructor
@Repository
public class ProductBundleRepository {

    private final Map<String, ProductBundle> nonStockItemIdMap = new HashMap<>();
    private final Map<String, ProductBundle> stockItemIdMap = new HashMap<>();

    public ProductBundle save(ProductBundle productBundle) {
        nonStockItemIdMap.put(productBundle.getNonStockItemId(), productBundle);
        productBundle.getStockItemsList().forEach(stockItem -> {
            stockItemIdMap.put(stockItem.getStockItemId(), productBundle);
        });
        log.trace("No of ProductBundle on repository is :{}", stockItemIdMap.size());
        return productBundle;
    }

    public ProductBundle findByNonStockItemId(String nonStockItemId) {
        return nonStockItemIdMap.get(nonStockItemId);
    }

    public ProductBundle findByStockItemID(String stockItemId) {
        return stockItemIdMap.get(stockItemId);
    }

    public List<ProductBundle> fetchPaginatedProductBundles(int page, int pageSize) {
        return getConsumedPage(nonStockItemIdMap.keySet().stream().toList(), page, pageSize)
            .stream().map(nonStockItemIdMap::get).toList();
    }
    public List<ProductBundle> findTerritoryPB(String territoryId) {
      return  nonStockItemIdMap.keySet().stream()
            .map(key->{
                Optional<TerritoryStatus> territoryPb = nonStockItemIdMap.get(key).getTerritoryStatusList().stream()
                    .filter(pbTerritory -> pbTerritory.getTerritoryId().equalsIgnoreCase(territoryId))
                    .findFirst();

                if(territoryPb.isPresent())return Optional.of(nonStockItemIdMap.get(key));
                Optional<ProductBundle> territoryEmptyPb=Optional.empty();
                return territoryEmptyPb;
            })
            .filter(Optional::isPresent)
            .map(Optional::get)
            .toList();
    }

    public int getSize() {
        return nonStockItemIdMap.size();
    }
}
