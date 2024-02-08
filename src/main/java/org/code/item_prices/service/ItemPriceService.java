package org.code.item_prices.service;


import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;


@Slf4j
@Service
@RequiredArgsConstructor
public class ItemPriceService {
    private final ItemPricesRepository itemPricesRepository;

    public ItemPrice saveItemPriceRecord(ItemPrice itemPrice) {
        itemPricesRepository.save(itemPrice);
        return itemPrice;
    }
}
