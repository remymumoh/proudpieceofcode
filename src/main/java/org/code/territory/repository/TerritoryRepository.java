package org.code.territory.repository;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Repository
@Slf4j
public class TerritoryRepository {

    private Map<String, Territory> territoryMap = new ConcurrentHashMap<>();
    private Map<String, Territory> priceListTerritoryMap = new ConcurrentHashMap<>();

    public Territory save(Territory territory) {
        this.territoryMap.put(territory.getId(), territory);
        this.priceListTerritoryMap.put(territory.getPriceListId(), territory);

        return territory;
    }

    public Territory findById(String territoryId) {
        return this.territoryMap.get(territoryId);
    }

    public Territory findByPriceListId(String priceListId) {
        return priceListTerritoryMap.get(priceListId);
    }
    public List<Territory> findAll() {
        return Lists.newArrayList(territoryMap.values());
    }

    public Map<String, Territory> findAllAsMap() {
        return territoryMap;
    }
    public int getSize(){
        return this.territoryMap.size();
    }
}
