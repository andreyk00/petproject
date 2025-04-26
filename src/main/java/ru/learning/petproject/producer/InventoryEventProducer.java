package ru.learning.petproject.producer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import ru.learning.petproject.dto.InventoryEventDto;
import ru.learning.petproject.entity.InventoryEvent;
import ru.learning.petproject.repository.InventoryEventRepository;

import java.util.concurrent.CompletableFuture;

@Component
@RequiredArgsConstructor
@Slf4j
public class InventoryEventProducer {

    @Value("${spring.kafka.topic}")
    private String topic;

    private final KafkaTemplate<Integer, InventoryEventDto> kafkaTemplate;
    private final InventoryEventRepository inventoryEventRepository;

    @KafkaListener(topicPartitions = @TopicPartition(topic = "inventory-event",
            partitionOffsets = {@PartitionOffset(partition = "0", initialOffset = "0")}))
    public void onMessage(ConsumerRecord<Integer, InventoryEventDto> consumerRecord) {
        try {
            log.info("Consumer Record key: {}", consumerRecord.key());

            InventoryEventDto eventDto = consumerRecord.value();
            if (eventDto == null) {
                log.error("Received null value from Kafka");
                return;
            }

            log.info("Consumer Record value: {}", eventDto);

            // Преобразуем DTO в сущность для сохранения в базу
            InventoryEvent event = InventoryEvent.builder()
                    .inventoryId(eventDto.getInventoryId())
                    .name(eventDto.getName())
                    .build();

            // Сохраняем данные в базу
            inventoryEventRepository.save(event);
            log.info("Data saved to database: {}", event);
        } catch (Exception e) {
            log.error("Failed to process message from Kafka", e);
        }
    }
}