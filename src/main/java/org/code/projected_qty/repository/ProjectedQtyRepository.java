package org.code.projected_qty.repository;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;



@Repository
@RequiredArgsConstructor
@Slf4j
public class ProjectedQtyRepository {

    private final Map<String, Map<String, ProjectedQty>> territoryIditemIdMap = new ConcurrentHashMap<>(100000);
    private final Map<String, ProjectedQty> idMap = new ConcurrentHashMap<>(100000);


    public ProjectedQty save(ProjectedQty projectedQty) {
        idMap.put(projectedQty.getId(), projectedQty);
        if (!territoryIditemIdMap.containsKey(projectedQty.getTerritoryId())) {
            territoryIditemIdMap.put(projectedQty.getTerritoryId(), new ConcurrentHashMap<>());
        }
        territoryIditemIdMap.get(projectedQty.getTerritoryId()).put(projectedQty.getItemId(), projectedQty);

        return projectedQty;
    }


    public ProjectedQty findById(String msgRef) {
        return idMap.get(msgRef);
    }

    public ProjectedQty findByItemId(String msgRef) {
        return idMap.get(msgRef);
    }

    public int getSize() {
        return this.idMap.size();
    }

    public ProjectedQty findByItemIdAndTerritory(String itemId, String territory) {
        return territoryIditemIdMap.get(territory).get(itemId);
    }

    public List<ProjectedQty> findByTerritory(String territory) {
        return territoryIditemIdMap.get(territory).keySet()
            .stream().map(key-> territoryIditemIdMap.get(territory).get(key)).toList();
    }

    public List<ProjectedQty> findByPaginatedItemId(String itemId, int page, int pageSize) {
        List<ProjectedQty> itemPQs = idMap.keySet().stream()
            .map(idMap::get)
            .filter(projectedQty -> projectedQty.getItemId().equalsIgnoreCase(itemId))
            .toList();
        return getConsumedPage(itemPQs, page, pageSize);
    }
}

