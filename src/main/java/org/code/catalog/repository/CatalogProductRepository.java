package org.code.catalog.repository;


import com.google.common.collect.Lists;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Repository;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Repository
@RequiredArgsConstructor
@Slf4j
public class CatalogProductRepository {

    private Map<String, Catalog> catalogMap;
    private Map<String, Map<String, List<Catalog>>> territoryStockItemMap;
    private Map<String, Map<String, Catalog>> pbTerritoryMap;
    private final CatalogIndex searchIndex;

    @PostConstruct
    void init() {
        this.resetRepo();
    }

    public Catalog save(Catalog catalog) {

        catalogMap.put(catalog.getId(), catalog);
        addToTerritoryStockItemMap(catalog);
        addToPbTerritoryMap(catalog);
        return catalog;
    }

    private void addToPbTerritoryMap(Catalog catalog) {
        if (!pbTerritoryMap.containsKey(catalog.getCodeProductBundleId())) {
            pbTerritoryMap.put(catalog.getCodeProductBundleId(), new ConcurrentHashMap<>());
        }
        pbTerritoryMap.get(catalog.getCodeProductBundleId()).put(catalog.getTerritoryId(), catalog);
    }

    private void addToTerritoryStockItemMap(Catalog catalog) {
        if (territoryStockItemMap.get(catalog.getTerritoryId()) == null) {
            territoryStockItemMap.put(catalog.getTerritoryId(), new ConcurrentHashMap<>());
        }
        catalog.getCatalogStockItemsList().stream().forEach(catalogStockItem -> {
            if (territoryStockItemMap.get(catalog.getTerritoryId()).get(catalogStockItem.getStockItemId()) == null) {
                territoryStockItemMap.get(catalog.getTerritoryId()).put(catalogStockItem.getStockItemId(), new ArrayList<Catalog>());
            }
            List<Catalog> catalogList = new ArrayList<>(territoryStockItemMap.get(catalog.getTerritoryId()).get(catalogStockItem.getStockItemId()));
            Optional<Catalog> catalogExist = catalogList
                .stream()
                .filter(catal -> Objects.equals(catal.getId(), catalog.getId()))
                .findFirst();

            if (catalogExist.isEmpty()) {
                catalogList.add(catalog);
            } else {
                catalogList = catalogList.stream()
                    .map(catalogItem -> {
                        if (Objects.equals(catalogItem.getId(), catalog.getId())) return catalog;
                        return catalogItem;
                    }).toList();
            }

            territoryStockItemMap.get(catalog.getTerritoryId()).put(catalogStockItem.getStockItemId(), catalogList);
        });
    }

    public List<Catalog> search(String searchTerm, String territoryId, int size) throws Exception {
        try {
            if (searchTerm.toLowerCase().startsWith(territoryId.trim().toLowerCase())) {
                log.debug("search catalogMap catalog id:{}", searchTerm);

                Catalog catalog = catalogMap.get(searchTerm);
                if (catalog != null) {
                    return List.of(catalog);
                } else {
                    return Collections.emptyList();
                }
            }

            log.debug("search lucene Index searchTerm:{}", searchTerm);
            var indexSearchTerm = searchTerm.toLowerCase();
            List<Catalog> catalogs = new ArrayList<>();
            try {
                catalogs = searchIndex.search(indexSearchTerm, size)
                    .stream()
                    .filter(pbTerritoryMap::containsKey)
                    .map(pbTerritoryMap::get)
                    .map(territoryCatMap -> territoryCatMap.get(territoryId))
                    .filter(Objects::nonNull)
                    .toList();
            } catch (Exception e) {
                log.error("No product bundle. Error searching for term: {}. Message: {}", searchTerm, e.getMessage());
            }
            if (catalogs.isEmpty()) {
                var territoryCatalogMap = territoryStockItemMap.getOrDefault(territoryId, Collections.emptyMap());

                if (territoryCatalogMap.isEmpty()) {
                    catalogs = Collections.emptyList();
                } else {
                    catalogs = territoryCatalogMap.values().stream()
                        .filter(Objects::nonNull)
                        .flatMap(Collection::stream)
                        .filter(Objects::nonNull)
                        .filter(catalog -> catalog.getId().toLowerCase().contains(indexSearchTerm))
                        .collect(Collectors.toList());
                }
            }

            return sortCatalog(catalogs);
        } catch (Exception e) {
            log.error("No product bundle. Error searching for term: {}. Message: {}", searchTerm, e.getMessage());
            throw new NotFoundException("No item found with name like {}", searchTerm);
        }
    }


    public void resetRepo() {
        this.catalogMap = new ConcurrentHashMap<>();
        this.territoryStockItemMap = new ConcurrentHashMap<>();
        this.pbTerritoryMap = new ConcurrentHashMap<>();
    }

    public List<Catalog> findByTerritoryIdAndStockItemId(String territoryId, String stockItemId) {
        if (territoryStockItemMap.get(territoryId) == null || territoryStockItemMap.get(territoryId).get(stockItemId) == null) {
            return Lists.newArrayList();
        }
        return territoryStockItemMap.get(territoryId).get(stockItemId);
    }

    public Optional<Catalog> findById(String territoryId, String nonStockItemId) {
        var id = territoryId.concat(nonStockItemId);
        return Optional.ofNullable(catalogMap.get(id));
    }

    public Optional<Catalog> findByCatalogId(String catalogId) {
        return Optional.ofNullable(catalogMap.get(catalogId));
    }

    public List<Catalog> findByTerritoryIdAndDisabled(String territoryId, boolean disabled) {
        var catalogTerritoryMap = territoryStockItemMap.get(territoryId);
        if (catalogTerritoryMap != null) {
            var catalogs = catalogTerritoryMap.keySet().stream()
                .map(catalogTerritoryMap::get)
                .flatMap(Collection::parallelStream)
                .filter(catalog -> catalog.getDisabled() == disabled)
                .toList();
            return sortCatalog(catalogs);
        } else {
            return new ArrayList<Catalog>();
        }
    }


    public List<Catalog> findByTerritoryId(String territoryId) {
        var catalogTerritoryMap = territoryStockItemMap.get(territoryId);
        if (catalogTerritoryMap != null) {
            var catalogs = catalogTerritoryMap.keySet().stream()
                .map(catalogTerritoryMap::get)
                .flatMap(Collection::parallelStream)
                .toList();
            return catalogs;
        } else {
            return new ArrayList<Catalog>();
        }

    }

    public List<Catalog> findByTerritoryIdCategoryIdAndDisabled(String territoryId, String categoryId, boolean disabled) {
        var catalogTerritoryMap = territoryStockItemMap.get(territoryId);
        if (catalogTerritoryMap != null) {
            var catalogs = catalogTerritoryMap.keySet().stream()
                .map(catalogTerritoryMap::get)
                .flatMap(Collection::parallelStream)
                .filter(catalog -> catalog.getDisabled() == disabled && catalog.getCategoryId().equalsIgnoreCase(categoryId))
                .toList();
            return sortCatalog(catalogs);
        } else {
            return new ArrayList<Catalog>();
        }
    }

    public int getSize() {
        log.info("No of Catalog Products created {}", this.catalogMap.size());
        log.debug("Catalog territories:");
        StringBuilder catalogTerritories = new StringBuilder();
        this.territoryStockItemMap.keySet()
            .forEach(territory -> catalogTerritories.append(territory).append("|"));
        log.debug(catalogTerritories.toString());
        return this.catalogMap.size();
    }

    private List<Catalog> sortCatalog(List<Catalog> catalogList) {
        List<Catalog> catalogSorted = catalogList;
        try {

            catalogSorted = catalogList.stream()
                .sorted((o1, o2) -> o1.getItemName().compareToIgnoreCase(o2.getItemName()))
                .toList();
        } catch (Exception e) {
            log.error("Catalog sorted Exception {}", e.getMessage());
        }
        return catalogSorted;

    }
}
