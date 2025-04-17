package ru.learning.petproject.dto;

import lombok.*;

/**
 *
 */
@Data
//@Builder(toBuilder = true)
//@Builder
@RequiredArgsConstructor//(access = AccessLevel.PRIVATE)
//@NoArgsConstructor//(access = AccessLevel.PRIVATE)
public class InventoryEvent {

    /**
     *
     */
    private Integer inventoryId;


    /**
     *
     */
    private String name;

}
