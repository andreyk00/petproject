package ru.learning.petproject.config;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.config.TopicBuilder;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Configuration
public class KafkaConfig {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConfig.class);

    @Value(value = "${spring.kafka.bootstrap-servers}")
    private String bootstrapAddress;

    @Value(value = "${spring.kafka.topic}")
    private String inventoryEventsTopicName;

    @Value(value = "${spring.kafka.consumer.auto-offset-reset:earliest}")
    private String autoOffsetReset;

    /**
     * Создает KafkaAdmin для управления Kafka (например, создание топиков).
     */
    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        logger.info("KafkaAdmin created with bootstrap servers: {}", bootstrapAddress);
        return new KafkaAdmin(configs);
    }

    /**
     * Создает топик Kafka с именем, указанным в application.properties.
     * Используются значения по умолчанию для количества партиций и реплик.
     */
    @Bean
    public NewTopic inventoryEventsTopic() {
        logger.info(
                "Creating topic '{}' with default partitions and replicas",
                inventoryEventsTopicName
        );
        return TopicBuilder.name(inventoryEventsTopicName)
                .build(); // Создает топик с настройками по умолчанию (1 партиция, 1 реплика)
    }
}