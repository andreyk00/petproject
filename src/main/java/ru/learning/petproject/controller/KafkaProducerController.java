package ru.learning.petproject.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import ru.learning.petproject.dto.InventoryEvent;
import ru.learning.petproject.service.TaskService;
import ru.learning.petproject.service.TestService;

@RestController
@RequiredArgsConstructor
@Slf4j
public class KafkaProducerController {

    private final KafkaTemplate<Integer, InventoryEvent> kafkaTemplate;

    private final TestService testService;

    private final TaskService taskService;

    @GetMapping("/send_message")
    public String sendMessageToKafka(@RequestParam("Id") Integer id,
                                     @RequestParam("name") String name) {
        //taskService.createNewTopic("inventory-event", 1, (short) 1);
        log.info("sendMessageToKafka: Id={}, name={}", id, name);
        testService.test();

        //Проверка выходных данных
        if (id == null || name == null || name.isEmpty()) {
            return "Invalid input: id and name are required.";
        }
        InventoryEvent event = InventoryEvent.builder()
                .inventoryId(id)
                .name(name)
                .build();

        try {
            // Отправляем событие в Kafka
            kafkaTemplate.send("inventory-event", id, event);
            log.info("Message sent to Kafka topic 'inventory-event': {}", event);
            return "Message sent to Kafka topic 'inventory-event': " + event;
        } catch (Exception e) {
            log.error("Failed to send message to Kafka", e);
            return "Failed to send message to Kafka: " + e.getMessage();
        }

    }

}
