package com.shah.learn.producer;

import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
@RequiredArgsConstructor
public class KafkaMessageProducer {

    private final KafkaTemplate<String,Object> kafkaTemplate;

    public void sendMessageToTopic(String message){
        CompletableFuture<SendResult<String, Object>> completableFuture = kafkaTemplate.send("test-topic-1", message);
        completableFuture.whenComplete((result,ex)->{
           if(ex==null){
               System.out.println("Sent message=["+message+"] with offset=["+
                       result.getRecordMetadata().offset()+"]");
           }
           else{
               System.out.println("Unable to send message=["
               +message+"] due to : "+ex.getMessage());
           }
        });
    }

}
