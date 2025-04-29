package ru.learning.petproject.dto;

import lombok.*;


@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class InventoryEventDto {

    private Integer inventoryId;
    private String name;
}
