package ru.learning.petproject.service;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class TestService {

    private final KafkaTemplate<?, ?> kafkaTemplate;

    public void test() {
        log.info("Hello from TestService!");
        String defaultTopic = kafkaTemplate.getDefaultTopic();
        if (defaultTopic != null) {
            log.info("Default Kafka topic: {}", defaultTopic);
        } else {
            log.warn("Default Kafka topic is not set.");
        }
    }
}