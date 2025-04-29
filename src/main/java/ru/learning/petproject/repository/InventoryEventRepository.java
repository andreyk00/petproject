package ru.learning.petproject.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import ru.learning.petproject.entity.InventoryEvent;

@Repository
public interface InventoryEventRepository extends JpaRepository<InventoryEvent, Integer> {

}
