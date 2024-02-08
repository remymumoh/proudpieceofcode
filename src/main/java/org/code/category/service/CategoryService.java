package org.code.category.service;


import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;


@Slf4j
@Service
@RequiredArgsConstructor
public class CategoryService {
    private final CategoryRepositoryImpl repository;
    private final CatalogPriceQtyRepository catalogPriceQtyRepository;
    private final CatalogProductRepository catalogProductRepository;


    public ItemGroup saveCategoryRecord(ItemGroup itemGroup) {
        if (itemGroup.getKyoskType().equals(String.valueOf(CategoyType.SUB_CATEGORY))) {
            return repository.save(itemGroup);
        }
        return null;
    }

    public GetItemCategoriesResponse fetchCategories(String territoryId) {
        if (ObjectUtils.isEmpty(territoryId)) {
            throw new BadRequestException("territoryId filter required!");
        }

        if (territoryId.equalsIgnoreCase("ALL")) {
            return GetItemCategoriesResponse.newBuilder()
                .addAllCategories(repository.findAll())
                .build();
        }
        boolean disabled = false;
        List<Catalog> catalogs = catalogProductRepository.findByTerritoryIdAndDisabled(territoryId, disabled);
//return null if there is no catalog for that territory
        if (catalogs.isEmpty()) {
            return GetItemCategoriesResponse.newBuilder().build();
        }
        //get the priceqty for the catalogs

        Set<String> catalogItemGroup = (Set<String>) fetchCategoryFromCatalogWithQty(catalogs);
        
        return GetItemCategoriesResponse.newBuilder()
            .addAllCategories(
                repository.findAll().stream()
                    .filter(category -> catalogItemGroup.contains(category.getId()))
                    .collect(Collectors.toSet())
            )
            .build();
    }

    private Set<?> fetchCategoryFromCatalogWithQty(List<Catalog> catalogs) {
        return catalogs.parallelStream()
            .map(catalog -> {
                Optional<CatalogProductPriceQty> priceQty = catalogPriceQtyRepository.findByCatalogProductId(catalog);
                if (priceQty.isEmpty() || (priceQty.get().getSellableQty() == 0)) return Optional.empty();
                return Optional.of(catalog.getCategoryId());

            })
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toSet());
    }
}
