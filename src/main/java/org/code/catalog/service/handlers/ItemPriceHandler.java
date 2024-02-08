package org.code.catalog.service.handlers;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Optional;

@Service
@RequiredArgsConstructor
@Slf4j
public class ItemPriceHandler {

    private final ItemPricesRepository itemPricesRepository;
    private final TerritoryRepository territoryRepository;
    private final CatalogPriceQtyRepository priceQtyRepository;

    public void processItemPrice(String msgRef) {
        log.info("ITEM_PRICE EVENT PROCESS Started{} FOR Item:{}", Instant.now(), msgRef);

        ItemPrice itemPrice = itemPricesRepository.findById(msgRef);
        Territory territory = territoryRepository.findByPriceListId(itemPrice.getPriceListId());
        if (territory == null || territory.getDisabled() || !territory.getId().equals(itemPrice.getPriceListId())) {
            // Sometimes the territory is disabled or a price list is wrongly assigned to another territory
            itemPricesRepository.saveMissingItemPriceList(itemPrice);
            log.error("Could not find Territory for price list {}", itemPrice.getPriceListId());
            return;
        }

        createUpdateCatalogPriceQty(CatalogUtil.getCatalogId(territory.getId(), itemPrice.getItemId()),
            itemPrice);

        log.trace("ITEM_PRICE CATALOG EVENT PROCESS FOR PB:{}", itemPrice.getId());
    }

    private void createUpdateCatalogPriceQty(String catalogProductId, ItemPrice itemPrice) {
        Optional<CatalogProductPriceQty> optionalPriceQty = priceQtyRepository.findByCatalogProductId(catalogProductId);
        CatalogProductPriceQty.Builder priceQtyBuilder = getPriceQtyBuilder(optionalPriceQty, catalogProductId);

        CatalogProductPriceQty priceQty = priceQtyBuilder
            .setSellingPrice(itemPrice.getPriceListRate())
            .setCurrency(itemPrice.getCurrency())
            .setPriceLastUpdated(CatalogUtils.getCurrentTime())
            .build();
        priceQtyRepository.save(priceQty);
    }

    private CatalogProductPriceQty.Builder getPriceQtyBuilder(Optional<CatalogProductPriceQty> priceQty,
                                                              String catalogProductId) {
        CatalogProductPriceQty.Builder priceQtyBuilder;
        if (priceQty.isPresent()) {
            priceQtyBuilder = CatalogProductPriceQty.newBuilder(priceQty.get());
        } else {
            log.error("No Item Price Qty for {}", catalogProductId);
            priceQtyBuilder = CatalogProductPriceQty.newBuilder();
            priceQtyBuilder.setCatalogProductId(catalogProductId);
        }
        return priceQtyBuilder;
    }



}
