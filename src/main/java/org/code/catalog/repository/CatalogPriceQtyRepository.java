package org.code.catalog.repository;


import org.springframework.stereotype.Repository;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Repository
public class CatalogPriceQtyRepository {

    private Map<String,CatalogProductPriceQty> priceQtyMap = new ConcurrentHashMap<>();

    public CatalogProductPriceQty save(CatalogProductPriceQty priceQty){
        priceQtyMap.put(priceQty.getCatalogProductId(),priceQty);
        return  priceQty;
    }

    public Optional<CatalogProductPriceQty> findByCatalogProductId(Catalog catalog) {
        return this.findByCatalogProductId(catalog.getId());
    }

    public Optional<CatalogProductPriceQty> findByCatalogProductId(String catalogProductId) {

        return Optional.ofNullable(priceQtyMap.get(catalogProductId));
    }


}
