package ru.learning.petproject.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import ru.learning.petproject.dto.InventoryEvent;

import java.util.concurrent.CompletableFuture;

@Slf4j
@Component
@RequiredArgsConstructor
public class InventoryEventProducer {

    @Value("${spring.kafka.topic}")
    private String topic;

    private final KafkaTemplate<Integer, InventoryEvent> kafkaTemplate;

    /**
     * Отправляет событие в Kafka.
     *
     * @param inventoryEvent событие для отправки
     * @return CompletableFuture для асинхронной обработки результата
     */
    public CompletableFuture<SendResult<Integer, InventoryEvent>> sendInventoryEvent(InventoryEvent inventoryEvent) {
        var key = inventoryEvent.getInventoryId();
        log.info("Sending event to Kafka topic '{}' with key: {}", topic, key);

        // Отправка сообщения в Kafka
        return kafkaTemplate.send(topic, key, inventoryEvent)
                .toCompletableFuture()
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to send message to Kafka topic '{}'", topic, ex);
                    } else {
                        log.info("Message sent successfully to Kafka topic '{}' with offset: {}",
                                topic, result.getRecordMetadata().offset());
                    }
                });
    }
}