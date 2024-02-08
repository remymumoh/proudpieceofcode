package org.code.category.repository;



import java.util.List;

public interface CategoryRepository {

    // should create or update
    public ItemGroup save(ItemGroup category);


    public List<ItemGroup> findAll();

    // Should perform a regex search on the name field
    public List<ItemGroup> findByNameLike(String name);


    int getSize();
}
