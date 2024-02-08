package org.code.catalog.service.handlers;

import lombok.NonNull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class CatalogUtil {

    public static String getCatalogId(@NonNull String territoryId, @NonNull String nonStockItemId){
        return territoryId.concat(nonStockItemId);
    }

    public static boolean isDisabled(ProductBundle productBundle,String territoryID){
        var territoryStatusMap = getTerritoryStatusMap(productBundle);
        return isDisabled(productBundle,territoryID,territoryStatusMap);
    }

    public static boolean isDisabled(ProductBundle productBundle,String territoryID,Map<String,Boolean>territoryStatusMap){
        if(productBundle.getDisabled()){
            return true;
        }

        if(!territoryStatusMap.containsKey(territoryID)){
            return true;
        }
        return !territoryStatusMap.get(territoryID);
    }

    public static Map<String, Boolean> getTerritoryStatusMap(ProductBundle productBundle) {
        if (Objects.isNull(productBundle)) {
            return new HashMap<>();
        }
        List<TerritoryStatus> territoryStatuslist = productBundle.getTerritoryStatusList();
        return territoryStatuslist
            .stream()
            .collect(Collectors.toMap(
                TerritoryStatus::getTerritoryId,
                TerritoryStatus::getActive,
                (a1, a2) -> a1 || a2));
    }

}
