package org.proudcode;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;


@Slf4j
@Service
@RequiredArgsConstructor
public class ProductService {
    private final ProductBundleRepository productRepository;

    public ProductBundle saveProductBundleRecord(ProductBundle productBundle) {
        return productRepository.save(productBundle);
    }
}
