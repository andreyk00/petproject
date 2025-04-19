package ru.learning.petproject.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import ru.learning.petproject.dto.InventoryEvent;

@RestController
@RequiredArgsConstructor
@Slf4j
public class KafkaProducerController {

    private final KafkaTemplate<Integer, InventoryEvent> kafkaTemplate;

    @GetMapping("/send_message")
    public String sendMessageToKafka(@RequestParam("Id") Integer id,
                                     @RequestParam("name") String name) {

        //Проверка выходных данных
        if (id == null || name == null || name.isEmpty()) {

            return "Invalid input: id and name are required.";
        }
        try {
            InventoryEvent event = InventoryEvent.builder()
                    .inventoryId(id)
                    .name(name)
                    .build();

            // Отправляем событие в Kafka
            kafkaTemplate.send("inventory-event", id, event);
            log.info("Message sent to Kafka topic 'inventory-events': {}", event);

            return "Message sent to Kafka topic 'inventory-events': " + event;
        } catch (Exception e) {
            log.error("Failed to send message to Kafka", e);
            return "Failed to send message to Kafka: " + e.getMessage();
        }


    }

}
