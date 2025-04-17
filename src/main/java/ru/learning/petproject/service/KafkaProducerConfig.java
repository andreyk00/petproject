package ru.learning.petproject.service;

import com.fasterxml.jackson.databind.JsonSerializer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.SendResult;
import ru.learning.petproject.dto.InventoryEvent;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Configuration
public class KafkaProducerConfig {

    @Value("${spring.kafka.producer.bootstrap-servers}")
    private List<String> bootstrapAddress;

    @Bean
    public ProducerFactory<?, ?> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    public KafkaTemplate<?, ?> kafkaTemplate() {
        //KafkaTemplate<Object, InventoryEvent> kafkaTemplate = new KafkaTemplate<Object, InventoryEvent>(producerFactory());
        KafkaTemplate<Integer, InventoryEvent> kafkaTemplate = new KafkaTemplate<Integer, InventoryEvent>((ProducerFactory<Integer, InventoryEvent>) producerFactory());
        System.out.println("creating kafkaTemplate: " + kafkaTemplate);
        System.out.println("kafkaTemplate.getDefaultTopic : " + kafkaTemplate.getDefaultTopic());
        //var event = InventoryEvent.builder().inventoryId(1).name("event 1").build();
        var event = new InventoryEvent();
        event.setInventoryId(1);
        event.setName("event 1");
        //Object

        //CompletableFuture<SendResult<Integer, InventoryEvent>> completableFuture =
        //        kafkaTemplate.send("inventory-events", 1, event);
        return kafkaTemplate;
    }

}
