package org.code.catalog.service;


import com.google.gson.Gson;
import com.google.protobuf.Struct;
import com.google.protobuf.util.JsonFormat;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;



enum PriceQtyEnum {
    SERVICE, APP
}

@Service
@Slf4j
@RequiredArgsConstructor
public class CatalogProductWrapperService {
    @Value("${org-code.duka}")
    private String dukaApp;
    @Value("${org-code.frontend}")
    private String frontendApp;
    private final CatalogProductRepository catalogProductRepository;
    private final CatalogPriceQtyRepository catalogPriceQtyRepository;
    private final TerritoryRepository territoryRepository;
    private final ItemPricesRepository itemPricesRepository;
    private final ProductBundleRepository productBundleRepository;
    private final ProjectedQtyRepository projectedQtyRepository;
    private Gson gson = new Gson();

    public CatalogProductResponse getCatalogItem(CatalogItemRequest request) throws Exception {

        String teritoryId = request.getTerritoryId();
        var territoryIds = request.getTerritoryIdsList();

        if (ObjectUtils.isEmpty(teritoryId) && ObjectUtils.isEmpty(territoryIds)) {
            throw new BadRequestException("You must provide Territory fields");
        }
        final int limit = isStringNullOrEmpty(String.valueOf(request.getRequestLimit())) ? 10 : request.getRequestLimit();
        String searchTerm = "";

        if (!isStringNullOrEmpty(request.getItemName())) {
            searchTerm = request.getItemName();
        } else if (!isStringNullOrEmpty(request.getCategoryId())) {
            searchTerm = request.getCategoryId();
        } else if (!isStringNullOrEmpty(request.getCatalogId())) {
            searchTerm = request.getCatalogId();
        }
        if (isStringNullOrEmpty(searchTerm)) {
            throw new catalog.catalog.exceptions.BadRequestException("You must provide searchTerm values for either[catalogId,itemName,categoryId] ");
        }

        if (ObjectUtils.isNotEmpty(teritoryId)) {
            List<Catalog> catalogs = catalogProductRepository.search(searchTerm, teritoryId, limit);
            List<CatalogProductWrapper> catalogProductWrappers = buildCatalogProductWrapper(catalogs, PriceQtyEnum.APP);
            return CatalogProductResponse.newBuilder()
                .addAllCatalog(catalogProductWrappers)
                .build();
        }
        List<TerritoryIdCatalogWrapper> territoryIdCatalogWrapperList = new ArrayList<>();
        if (!territoryIds.isEmpty()) {
            List<List<Catalog>> teritoryCatalogs = new ArrayList<>();
            for (String tId : territoryIds) {
                List<Catalog> catalogs = catalogProductRepository.search(searchTerm, tId, limit);
                teritoryCatalogs.add(catalogs);
            }
            List<Catalog> teritoryCatalogsList = teritoryCatalogs.stream()
                .flatMap(Collection::stream)
                .toList();
            territoryIdCatalogWrapperList = buildCatalogGroupedByItemName(teritoryCatalogsList, PriceQtyEnum.APP);
        }


        return CatalogProductResponse.newBuilder()
            .addAllItemCatalogs(territoryIdCatalogWrapperList)
            .build();
    }

    private List<TerritoryIdCatalogWrapper> buildCatalogGroupedByItemName(List<Catalog> teritoryCatalogs, PriceQtyEnum app) {
        List<CatalogProductWrapper> catalogProductWrappers = buildCatalogProductWrapper(teritoryCatalogs, PriceQtyEnum.APP);
        List<TerritoryIdCatalogWrapper> territoryIdCatalogWrapperList = new ArrayList<>();
        var catalogItems = catalogProductWrappers.stream()
            .map(catalog -> catalog.getCatalog().getItemName())
            .distinct()
            .sorted()
            .toList();

        catalogItems.forEach(itemName -> {
            var itemNameCatalogs = catalogProductWrappers.stream()
                .filter(catalog -> catalog.getCatalog().getItemName().equals(itemName))
                .toList();
            TerritoryIdCatalogWrapper wrapper = TerritoryIdCatalogWrapper.newBuilder()
                .setItemName(itemName)
                .addAllCatalog(itemNameCatalogs)
                .build();
            territoryIdCatalogWrapperList.add(wrapper);
        });
        return territoryIdCatalogWrapperList;
    }


    public CatalogProductResponse getCatalogByCatalogIdFilter(CatalogIdFilter request) {

        List<Catalog> catalogs = getCatalogsFromFilter(request.getCatalogId());

        List<CatalogProductWrapper> catalogProductWrappers = buildCatalogProductWrapper(catalogs, PriceQtyEnum.SERVICE);
        return CatalogProductResponse.newBuilder()
            .addAllCatalog(catalogProductWrappers)
            .build();
    }


    private List<Catalog> getCatalogsFromFilter(Filter input) {
        switch (input.getOperator()) {
            case EQUALS:
                var catalog = catalogProductRepository.findByCatalogId(input.getValue());
                return catalog.isPresent() ? List.of(catalog.get()) : new ArrayList<>();
            case IN:
                return input.getValuesList().parallelStream()
                    .map(catalogProductRepository::findByCatalogId)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .toList();
            default:
                throw new BadRequestException("Filter Operation [" + input.getOperator() + "] not supported yet");
        }
    }

    private List<CatalogProductWrapper> buildCatalogProductWrapper(List<Catalog> catalogs, PriceQtyEnum priceQtyEnum) {
        var catalogWrapperList = catalogs.parallelStream()
            .map(catalog -> {
                var priceQty = catalogPriceQtyRepository.findByCatalogProductId(catalog);

                CatalogProductWrapper.Builder catalogWrapper = CatalogProductWrapper.newBuilder()
                    .setCatalog(catalog);
                if (priceQty.isPresent()) {
                    var pqty = CatalogProductPriceQty.newBuilder(priceQty.get());
                    if (ObjectUtils.isEmpty(priceQty.get().getSellableQty())) {
                        pqty.setSellableQty(0);
                    }
                    if (ObjectUtils.isEmpty(priceQty.get().getSellingPrice())) {
                        pqty.setSellingPrice(0);
                    }
                    catalogWrapper.setQtyPrice(pqty.build());
                } else {
                    catalogWrapper.setQtyPrice(CatalogProductPriceQty.newBuilder()
                        .setCatalogProductId(catalog.getId())
                        .setSellingPrice(0)
                        .setSellableQty(0)
                        .build());
                }

                return catalogWrapper.build();
            })
            .toList();

//Filter catalog based on price and qty
        List<CatalogProductWrapper> catalogProductWrappers = new ArrayList<>();
        catalogWrapperList.stream().forEach(catWrap -> {
            var hasPriceQty = priceAndQtyFilter(catWrap, priceQtyEnum);
            if (hasPriceQty) catalogProductWrappers.add(catWrap);
        });
        return catalogProductWrappers;


    }

    private boolean priceAndQtyFilter(CatalogProductWrapper catalogProductWrapper, PriceQtyEnum priceQtyEnum) {
        var token = SecurityUtils.getTokenUserDetails();
        var appCreated = token.getAppSource();
        var priceQty = catalogProductWrapper.getQtyPrice();

        if (priceQtyEnum.equals(PriceQtyEnum.SERVICE)) return true;

        if (Objects.equals(dukaApp, appCreated)) {
            return (priceQty.getSellingPrice() > 0 && priceQty.getSellableQty() > 0);
        } else if (Objects.equals(frontendApp, appCreated)) {
            return true;
        } else {
            return false;
        }
    }

    public GetPaginatedCatalogItemResponse getPaginatedCatalogItems(GetPaginatedCatalogItemRequest request) throws Exception {
        var token = SecurityUtils.getTokenUserDetails();
        var appCreated = token.getAppSource();
        String teritoryId = getFilterValue(request.getFilterList(), "territoryId");
        if (teritoryId == null) {
            throw new BadRequestException("Filter territoryId must be provided");
        }
        String categoryId = getFilterValue(request.getFilterList(), "categoryId");
        String disabled = getFilterValue(request.getFilterList(), "disabled");
        boolean isDisabled = Boolean.parseBoolean(disabled);
        String itemName = getFilterValue(request.getFilterList(), "itemName");


        List<Catalog> catalogs;

        if (StringUtils.hasText(itemName)) {
            catalogs = catalogProductRepository.search(itemName, teritoryId, request.getSize());

            catalogs = catalogs.stream().filter(catalog -> (isDisabled == catalog.getDisabled())).toList();
        } else {
            catalogs = catalogProductRepository.findByTerritoryIdAndDisabled(teritoryId, isDisabled);
        }

        if (categoryId != null) {
            catalogs = catalogProductRepository.findByTerritoryIdCategoryIdAndDisabled(teritoryId, categoryId, isDisabled);
        }
        if (catalogs != null) {

            Pagination.Builder pagination = catalogPagination(catalogs.size(), request);
            List<CatalogProductWrapper> catalogProductWrappers = new ArrayList<>();
            int pageNo = pagination.getPageNumber();
            int pages = pagination.getTotalPages();
            boolean noValidPage = true;
            while (noValidPage) {
                List<Catalog> catalogPage = getPage(catalogs, pageNo, pagination.getPageSize());
                catalogProductWrappers = buildCatalogProductWrapper(catalogPage, PriceQtyEnum.APP);
                log.debug("catalogProductWrappers size:{}", catalogProductWrappers.size());
                log.debug("appCreated:{}", appCreated);
                log.debug("pageNo:{}", pageNo);
                log.debug("Condition evlautaion:{}", (Objects.equals(dukaApp, appCreated) && catalogProductWrappers.isEmpty() && (pageNo < pages)));
                if (Objects.equals(dukaApp, appCreated) && catalogProductWrappers.isEmpty() && (pageNo < pages)) {
                    pageNo++;
                } else {
                    noValidPage = false;
                }
            }
            pagination.setNoOfElements(catalogProductWrappers.size());
            return GetPaginatedCatalogItemResponse.newBuilder()
                .addAllCatalogItems(catalogProductWrappers)
                .setPagination(pagination)
                .build();
        } else {
            return GetPaginatedCatalogItemResponse.newBuilder()
                .build();
        }
    }

    private List<Catalog> getPage(List<Catalog> sourceList, int page, int pageSize) {
        if (page <= 0) {
            page = 1;
        }
        if (pageSize <= 0) {
            pageSize = 10;
        }

        int fromIndex = (page - 1) * pageSize;
        if (sourceList == null || sourceList.size() <= fromIndex) {
            return Collections.emptyList();
        }
        // toIndex exclusive
        return sourceList.subList(fromIndex, Math.min(fromIndex + pageSize, sourceList.size()));
    }

    private Pagination.Builder catalogPagination(int listSize, GetPaginatedCatalogItemRequest request) {
        int page = request.getPage();
        int pageSize = request.getSize();
        int pages = getPages(listSize, pageSize);
        log.trace("page:{} listSize:{} pageSize:{} pages:{}", page, listSize, pageSize, (listSize % pageSize), pages);
        return Pagination.newBuilder()
            .setPageNumber(page)
            .setPageSize(pageSize)
            .setNoOfElements(pageSize)
            .setTotalElements(listSize)
            .setTotalPages(pages);

    }

    private int getPages(int listSize, int pageSize) {
        int overFlow = (listSize % pageSize);
        if (overFlow > 0) {
            return ((listSize - overFlow) / pageSize) + 1;
        } else {
            return listSize / pageSize;
        }
    }

    private String getFilterValue(List<Filter> filters, String field) {
        log.debug("field:[{}] filters:{}", field, filters);
        try {
            return filters.stream()
                .filter(filter -> filter.getField().equalsIgnoreCase(field))
                .findFirst()
                .get()
                .getValue();
        } catch (NoSuchElementException ex) {
            return null;
        }

    }

    /**
     * Query for consumed product-bundle-dp, item-prices-dp,  territory-dp, projected-qty-dp
     * QueryType{
     * PB = 0;//product-bundle-dp
     * IP = 1;//item-prices-dp
     * TE = 2;//territory-dp
     * PQ = 3;//projected-qty-dp
     * }
     *
     * @param request
     * @return
     */
    public ConsumedDPResponse getPaginatedDataProducts(FetchConsumedDP request) {
        log.debug("FetchConsumedDP:\n{}", request);
        QueryType dpToQuery = request.getDpToQuery();
        log.debug("QueryType:{}", dpToQuery);
        switch (dpToQuery) {
            case PB -> {

                return fetchQueryProductBundles(request);
            }
            case IP -> {
                return fetchItemPrices(request);
            }
            case TE -> {
                return fetchTerritories(request);
            }
            case PQ -> {
                return fetchQueryProjectQuantities(request);
            }
            default -> {
                return ConsumedDPResponse.newBuilder()
                    .setData(Struct.newBuilder()
                        .putFields(dpToQuery.toString(), com.google.protobuf.Value.newBuilder()
                            .setStringValue("No such [" + dpToQuery + "] data product found").build())
                        .build())
                    .build();
            }
        }

    }

    private ConsumedDPResponse fetchTerritories(FetchConsumedDP request) {
        Set<String> values = request.getFilterIdInList().stream().collect(Collectors.toSet());

        List<Territory> territoryList = territoryRepository.findAll()
            .stream()
            .filter(territory -> values.contains(territory.getId()))
            .toList();

        if (territoryList.isEmpty()) {
            territoryList = territoryRepository.findAll();
        }
        var territoryMap = getConsumedPage(territoryList, request.getPage(), request.getSize())
            .stream()
            .collect(Collectors.toMap(Territory::getId, Function.identity()));
        try {
            String territoryStr = gson.toJson(territoryMap);
            log.debug("Territory :{}", territoryStr);
            Struct.Builder territoryStruct = Struct.newBuilder();

            JsonFormat.parser().ignoringUnknownFields().merge(territoryStr, territoryStruct);
            Pagination.Builder pagination = queryPagination(territoryMap.size(), request);
            return ConsumedDPResponse.newBuilder().setData(territoryStruct.build()).setPagination(pagination).build();
        } catch (IOException e) {
            log.error("fetchTerritories Exception :{}", e.getMessage());
            return ConsumedDPResponse.newBuilder()
                .setData(Struct.newBuilder()
                    .putFields(request.getDpToQuery().toString(), com.google.protobuf.Value.newBuilder()
                        .setStringValue("fetchTerritories Exception :" + e.getMessage()).build())
                    .build())
                .build();
        }

    }

    private ConsumedDPResponse fetchItemPrices(FetchConsumedDP request) {
        if (request.getFilterIdInList().isEmpty())
            return ConsumedDPResponse.newBuilder()
                .setData(Struct.newBuilder()
                    .putFields(request.getDpToQuery().toString(), com.google.protobuf.Value.newBuilder()
                        .setStringValue("To query Item prices Provide the following:" +
                            "{filter_id_in: [ itemId  ,priceList]}itemId is COMPULSORY!!! " +
                            ".If priceList placeholder is set to ALL it will return a paginated list of item Prices for itemId.").build())
                    .build())
                .build();
//              {filter_id_in: [ itemId  ,priceList]}itemId is COMPULSORY!!!
        String itemId = request.getFilterIdInList().get(0);
        log.debug("itemId in index 0 is :{}", itemId);
        String priceList = request.getFilterIdInList().get(1);
        log.debug("priceList in index 1 is :{}", priceList);
        Struct.Builder itemPricesStruct = Struct.newBuilder();
        try {
            if (!priceList.equalsIgnoreCase("ALL")) {
                ItemPrice itemPrice = itemPricesRepository.findByItemIdAndPriceList(itemId, priceList);
                HashMap<String, ItemPrice> itemPricesMap = new HashMap<>();
                itemPricesMap.put(itemId, itemPrice);
                JsonFormat.parser().ignoringUnknownFields().merge(gson.toJson(itemPricesMap), itemPricesStruct);

                return ConsumedDPResponse.newBuilder().setData(itemPricesStruct.build()).build();
            }
//            If priceList placeholder is set to ALL it will return a paginated list of item Prices for itemId.
            List<ItemPrice> itemPricesList = itemPricesRepository.findByItemId(itemId);
            var itemPricesMap = getConsumedPage(itemPricesList, request.getPage(), request.getSize())
                .stream()
                .collect(Collectors.toMap(ItemPrice::getPriceListId, Function.identity()));

            log.debug("itemPricesMap :{}", itemPricesMap);
            String itemPriceStr = gson.toJson(itemPricesMap);

            JsonFormat.parser().ignoringUnknownFields().merge(itemPriceStr, itemPricesStruct);
            Pagination.Builder pagination = queryPagination(itemPricesRepository.getSize(), request);
            return ConsumedDPResponse.newBuilder().setData(itemPricesStruct.build()).setPagination(pagination).build();
        } catch (IOException e) {
            log.error("Item Prices Exception :{}", e.getMessage());
            return ConsumedDPResponse.newBuilder()
                .setData(Struct.newBuilder()
                    .putFields(request.getDpToQuery().toString(), com.google.protobuf.Value.newBuilder()
                        .setStringValue("fetchItemPrices Exception :" + e.getMessage()).build())
                    .build())
                .build();
        }

    }

    private ConsumedDPResponse fetchQueryProductBundles(FetchConsumedDP request) {

        List<ProductBundle> productBundleList = new ArrayList<>();
        ConsumedDPResponse.Builder consumedPDResponse = ConsumedDPResponse.newBuilder();
        if (request.getFilterIdInList().isEmpty()) {
            productBundleList = productBundleRepository.fetchPaginatedProductBundles(request.getPage(), request.getSize());
            Pagination.Builder pagination = queryPagination(productBundleRepository.getSize(), request);
            consumedPDResponse.setPagination(pagination);
        } else {
            for (String nonStockItemId : request.getFilterIdInList()) {
                ProductBundle productBundle = productBundleRepository.findByNonStockItemId(nonStockItemId);
                if (productBundle != null) productBundleList.add(productBundle);
            }
        }

        try {

            var productBundleMap = productBundleList.stream()
                .collect(Collectors.toMap(ProductBundle::getNonStockItemId, Function.identity()));
            String itemPriceStr = gson.toJson(productBundleMap);
            Struct.Builder productBundleStruct = Struct.newBuilder();

            JsonFormat.parser().ignoringUnknownFields().merge(itemPriceStr, productBundleStruct);

            return consumedPDResponse.setData(productBundleStruct.build()).build();

        } catch (IOException e) {
            log.error("Item Prices Exception :{}", e.getMessage());
            return ConsumedDPResponse.newBuilder()
                .setData(Struct.newBuilder()
                    .putFields(request.getDpToQuery().toString(), com.google.protobuf.Value.newBuilder()
                        .setStringValue("fetchItemPrices Exception :" + e.getMessage()).build())
                    .build())
                .build();
        }
    }

    private ConsumedDPResponse fetchQueryProjectQuantities(FetchConsumedDP request) {

        if (request.getFilterIdInList().isEmpty())
            return ConsumedDPResponse.newBuilder()
                .setData(Struct.newBuilder()
                    .putFields(request.getDpToQuery().toString(), com.google.protobuf.Value.newBuilder()
                        .setStringValue("To query Projected Qty Provide the following:" +
                            "{filter_id_in: [ itemId  ,territory]}itemId is COMPULSORY!!! " +
                            ".If territory placeholder is set to ALL it will return a paginated list of Projected Qty for itemId.").build())
                    .build())
                .build();
//              {filter_id_in: [ itemId  ,priceList]}itemId is COMPULSORY!!!
        String itemId = request.getFilterIdInList().get(0);
        log.debug("itemId in index 0 is :{}", itemId);
        String territory = request.getFilterIdInList().get(1);
        log.debug("territory in index 1 is :{}", territory);
        Struct.Builder itemPricesStruct = Struct.newBuilder();
        try {
            if (!territory.equalsIgnoreCase("ALL")) {
                ProjectedQty projectedQty = projectedQtyRepository.findByItemIdAndTerritory(itemId, territory);
                HashMap<String, ProjectedQty> itemPricesMap = new HashMap<>();
                itemPricesMap.put(projectedQty.getId(), projectedQty);
                JsonFormat.parser().ignoringUnknownFields().merge(gson.toJson(itemPricesMap), itemPricesStruct);

                return ConsumedDPResponse.newBuilder().setData(itemPricesStruct.build()).build();
            }
//            If territory placeholder is set to ALL it will return a paginated list of ProjectedQty for itemId.
            List<ProjectedQty> projectedQtyList = projectedQtyRepository.findByPaginatedItemId(itemId, request.getPage(), request.getSize());
            var projectedQtyMap = projectedQtyList
                .stream()
                .collect(Collectors.toMap(ProjectedQty::getId, Function.identity()));

            log.debug("itemPricesMap :{}", projectedQtyMap);
            String itemPriceStr = gson.toJson(projectedQtyMap);

            JsonFormat.parser().ignoringUnknownFields().merge(itemPriceStr, itemPricesStruct);
            Pagination.Builder pagination = queryPagination(projectedQtyRepository.getSize(), request);
            return ConsumedDPResponse.newBuilder().setData(itemPricesStruct.build()).setPagination(pagination).build();
        } catch (IOException e) {
            log.error("Projected Qty Exception :{}", e.getMessage());
            return ConsumedDPResponse.newBuilder()
                .setData(Struct.newBuilder()
                    .putFields(request.getDpToQuery().toString(), com.google.protobuf.Value.newBuilder()
                        .setStringValue("fetchQueryProjectQuantities Exception :" + e.getMessage()).build())
                    .build())
                .build();
        }
    }

    private Pagination.Builder queryPagination(int listSize, FetchConsumedDP request) {
        int page = request.getPage();
        int pageSize = request.getSize();
        int pages = getPages(listSize, pageSize);
        log.trace("page:{} listSize:{} pageSize:{} pages:{}", page, listSize, pageSize, (listSize % pageSize), pages);
        return Pagination.newBuilder()
            .setPageNumber(page)
            .setPageSize(pageSize)
            .setNoOfElements(pageSize)
            .setTotalElements(listSize)
            .setTotalPages(pages);

    }


}
