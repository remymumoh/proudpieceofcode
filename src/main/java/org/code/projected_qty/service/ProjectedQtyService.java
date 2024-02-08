package org.code.projected_qty.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;


@Slf4j
@Service
@RequiredArgsConstructor
public class ProjectedQtyService {
    private final ProjectedQtyRepository projectedQtyRepository;

    public ProjectedQty saveProjectedQtyRecord(ProjectedQty projectedQty) {
        projectedQtyRepository.save(projectedQty);
        return projectedQty;
    }
}
