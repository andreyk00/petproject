package ru.learning.petproject.service;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class TestService {

    @Value("${spring.kafka.topic}")
    private String topic;

    private final KafkaTemplate<?, ?> kafkaTemplate;

    public void test() {
        log.info("Hello from TestService!");
        kafkaTemplate.setDefaultTopic(topic);
        String defaultTopic = kafkaTemplate.getDefaultTopic();
        log.info("Default Kafka topic: {}", defaultTopic);
    }
}