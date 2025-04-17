package ru.learning.petproject.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import ru.learning.petproject.dto.InventoryEvent;

import java.util.concurrent.CompletableFuture;

@Slf4j
@Component
public class InventoryEventProducer {

    @Value(value = "${spring.kafka.topic}")
    public String topic;

    @Autowired
    private KafkaTemplate<Integer, Object> kafkaTemplate;

    public CompletableFuture<SendResult<Integer, Object>> sendInventoryEvent(InventoryEvent inventoryEvent) throws JsonProcessingException {
        var key = 1; //inventoryEvent.getInventoryId();
        var completableFuture = kafkaTemplate.send(topic, key, inventoryEvent);
        return completableFuture;
    }

}