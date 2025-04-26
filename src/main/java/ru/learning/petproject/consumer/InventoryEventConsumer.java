package ru.learning.petproject.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.stereotype.Component;
import ru.learning.petproject.dto.InventoryEventDto;

@Component
@Slf4j
public class InventoryEventConsumer {

    /* @KafkaListener(topics = {"inventory-event"})
    public void onMessage(ConsumerRecord<Integer, InventoryEvent> consumerRecord) {
        log.info("Consumer Record: {}", consumerRecord);
    }
     */

    /*@KafkaListener(
            topicPartitions = @TopicPartition(topic = "topicName",
                    partitionOffsets = {
                            @PartitionOffset(partition = "0", initialOffset = "0"),
                            @PartitionOffset(partition = "3", initialOffset = "0")}),
            containerFactory = "partitionsKafkaListenerContainerFactory")
    public void listenToPartition(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
        System.out.println(
                "Received Message: " + message"
                        + "from partition: " + partition);
    } */

    @KafkaListener(topicPartitions = @TopicPartition(topic = "inventory-event",
            partitionOffsets = {@PartitionOffset(partition = "0", initialOffset = "0")}))
    public void onMessage(ConsumerRecord<Integer, InventoryEventDto> consumerRecord) {
        //log.info("Consumer Record key: {}", consumerRecord.key());

        log.info("Consumer Record: {}", consumerRecord);
    }

}
