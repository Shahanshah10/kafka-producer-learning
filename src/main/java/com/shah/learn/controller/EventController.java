package com.shah.learn.controller;

import com.shah.learn.dto.Customer;
import com.shah.learn.producer.KafkaMessageProducer;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
@RequestMapping("/v1.0/producer-app")
public class EventController {

    private final KafkaMessageProducer kafkaMessageProducer;

    @PostMapping(value = "/publishMessage")
    public ResponseEntity<?> publishMessage(@RequestBody String message){
        try {
            kafkaMessageProducer.sendMessageToTopic(message);
            return ResponseEntity.status(HttpStatus.OK).body("Message is successfully publish to topic...");
        }catch (Exception exception){
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    @PostMapping(value = "/publishCustomerInfo")
    public ResponseEntity<?> publishCustomerInfo(@RequestBody Customer customer){
        try {
            kafkaMessageProducer.sendCustomerToTopic(customer);
            return ResponseEntity.status(HttpStatus.OK).body("Message is successfully publish to topic...");
        }catch (Exception exception){
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }


}
