package org.code.category.repository;


import com.google.common.collect.Lists;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Repository;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RequiredArgsConstructor
@Repository
@Slf4j
public class CategoryRepositoryImpl implements CategoryRepository{

    private final Map<String, ItemGroup> categoryIdMap= new HashMap();

    @Override
    public ItemGroup save(ItemGroup category) {
        categoryIdMap.put(category.getId(),category);
        return category;
    }

    @Override
    public List<ItemGroup> findAll() {
        return Lists.newArrayList(categoryIdMap.values());
    }

    @Override
    public List<ItemGroup> findByNameLike(String name) {
        // naive implementation with linear complexity
        List<ItemGroup> result = categoryIdMap.values()
            .stream()
            .filter(cat -> cat.getName().contains(name))
            .collect(Collectors.toList());
        return result;
    }

    @Override
    public int getSize(){
        return this.categoryIdMap.size();
    }
}
