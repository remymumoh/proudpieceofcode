package org.code.item_prices.repository;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Repository;
import org.springframework.util.ObjectUtils;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Repository
@Slf4j
public class ItemPricesRepository {

    private final Map<String, ItemPrice> itemPriceMap = new ConcurrentHashMap<>();
    private final Map<String, ItemPrice> missingItemPriceMap = new ConcurrentHashMap<>();
    private final Map<String, ItemPrice> zeroItemPriceMap = new ConcurrentHashMap<>();
    private final Map<String, Map<String, ItemPrice>> itemIdPriceListMap = new ConcurrentHashMap<>();

    public void saveMissingItemPriceList(ItemPrice itemPrice) {
        missingItemPriceMap.put(itemPrice.getId(), itemPrice);
    }

    public void deleteMissingItemPriceList(ItemPrice itemPrice) {
        missingItemPriceMap.remove(itemPrice.getId());
    } public void deleteZeroItemPriceList(ItemPrice itemPrice) {
        zeroItemPriceMap.remove(itemPrice.getId());
    }

    public List<ItemPrice> findAllZeroItemPrices() {
        return zeroItemPriceMap.keySet()
            .stream().map(key -> zeroItemPriceMap.get(key)).toList();
    }   public List<ItemPrice> findAllMissingItemPrices() {
        return missingItemPriceMap.keySet()
            .stream().map(key -> missingItemPriceMap.get(key)).toList();
    }

    public Optional<ItemPrice> save(ItemPrice itemPrice) {
        // Validate incoming price is newer than existing price
        if(itemPriceMap.get(itemPrice.getId()) != null){
            ItemPrice currentPrice = itemPriceMap.get(itemPrice.getId());
            if(dateIsValid(currentPrice.getValidFrom()) && dateIsValid(itemPrice.getValidFrom())){
                if(toDate(currentPrice.getValidFrom()).isAfter(toDate(itemPrice.getValidFrom()))){
                    return Optional.empty();
                }
            }
        }
        itemPriceMap.put(itemPrice.getId(), itemPrice);
        if (itemIdPriceListMap.get(itemPrice.getItemId()) == null) {
            itemIdPriceListMap.put(itemPrice.getItemId(), new HashMap<>());
        }
        itemIdPriceListMap.get(itemPrice.getItemId()).put(itemPrice.getPriceListId(), itemPrice);
//        log item prices with zero price
        var priceRate = ObjectUtils.isEmpty(itemPrice.getPriceListRate()) ? 0 : itemPrice.getPriceListRate();
        if (priceRate >= 0) zeroItemPriceMap.put(itemPrice.getId(), itemPrice);
        return Optional.of(itemPrice);



    }
    public LocalDate toDate(String dateString){
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        return LocalDate.parse(dateString, formatter);
    }
    public Boolean dateIsValid(String dateString){
        if(StringUtils.isEmpty(dateString)){
            return false;
        }
        String regexPattern = "\\d{4}-\\d{2}-\\d{2}";
        Pattern pattern = Pattern.compile(regexPattern);
        Matcher matcher = pattern.matcher(dateString);
        return matcher.matches();
    }



    public ItemPrice findById(String id) {
        return itemPriceMap.get(id);
    }

    public List<ItemPrice> findByItemId(String itemId) {
        if (itemIdPriceListMap.containsKey(itemId)) {
            return Lists.newArrayList(itemIdPriceListMap.get(itemId).values());
        }
        return Collections.emptyList();
    }

    public ItemPrice findByItemIdAndPriceList(String itemId, String priceList) {
        if (!itemIdPriceListMap.containsKey(itemId)) {
            return null;
        }
        return itemIdPriceListMap.get(itemId).get(priceList);
    }

    public List<ItemPrice> findByItemIdAndPriceList(String priceList) {
        return itemIdPriceListMap.keySet().stream()
            .map(key -> {
                if (!itemIdPriceListMap.get(key).containsKey(priceList)) {
                    Optional<ItemPrice> optional = Optional.empty();

                    return optional;
                }
                ItemPrice priceListVal = itemIdPriceListMap.get(key).get(priceList);
                return Optional.of(priceListVal);
            })
            .filter(Optional::isPresent)
            .map(Optional::get)
            .toList();
    }

    public int getSize() {
        return this.itemPriceMap.size();
    }

    public int getNoPriceListSize() {
        return this.missingItemPriceMap.size();
    }

    public int getZeroPriceSize() {
        return this.zeroItemPriceMap.size();
    }

}
