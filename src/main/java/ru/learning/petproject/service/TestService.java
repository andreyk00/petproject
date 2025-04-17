package ru.learning.petproject.service;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class TestService {

    @Autowired
    private KafkaTemplate kafkaTemplate;

    void test() {
        System.out.println("hello!");
        System.out.println(kafkaTemplate.getDefaultTopic());
    }

}
