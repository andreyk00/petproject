package ru.learning.petproject.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

@Slf4j
@Service
public class KafkaService {

    @Autowired
    private KafkaAdmin kafkaAdmin;

    @Value("${kafka.topic.retention.ms:86400000}")
    private long retentionMs;

    public void createNewTopic(String topicName, int partitions, short replicas) {
        Map<String, String> topicConfig = new HashMap<>();
        topicConfig.put(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(retentionMs));

        NewTopic newTopic = new NewTopic(topicName, partitions, replicas).configs(topicConfig);

        try (AdminClient adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            // Проверяем, существует ли топик
            Set<String> existingTopics = adminClient.listTopics().names().get();
            if (!((Set<?>) existingTopics).contains(topicName)) {
                log.info("Creating new Kafka topic: {}", topicName);
                adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
                log.info("Successfully created Kafka topic: {}", topicName);
            } else {
                log.info("Kafka topic '{}' already exists", topicName);
            }
        } catch (InterruptedException | ExecutionException e) {
            log.error("Failed to create Kafka topic '{}'", topicName, e);
            Thread.currentThread().interrupt();
        }
    }
}