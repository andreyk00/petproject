package ru.learning.petproject.dto;

import lombok.*;

/**
 *
 */
@Data
@Builder(toBuilder = true)
@NoArgsConstructor(force = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class InventoryEvent {

    /**
     *
     */
   private final Integer inventoryId;


    /**
     *
     */
    private final String name;

}
