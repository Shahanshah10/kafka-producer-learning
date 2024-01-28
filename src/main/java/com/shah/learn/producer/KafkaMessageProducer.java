package com.shah.learn.producer;

import com.shah.learn.dto.Customer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaMessageProducer {

    private final KafkaTemplate<String,Object> kafkaTemplate;

    public void sendMessageToTopic(String message){
        CompletableFuture<SendResult<String, Object>> completableFuture = kafkaTemplate.send("test-topic-1", message);
        completableFuture.whenComplete((result,ex)->{
           if(ex==null){
               log.info("Sent message=["+message+"] with offset=["+
                       result.getRecordMetadata().offset()+"]");
           }
           else{
               log.error("Unable to send message=["
               +message+"] due to : "+ex.getMessage());
           }
        });
    }

    public void sendCustomerToTopic(Customer customer){
        CompletableFuture<SendResult<String, Object>> completableFuture = kafkaTemplate.send("customer-topic", customer);
        completableFuture.whenComplete((result,ex)->{
            if(ex==null){
                log.info("Sent Customer info :{} with offset: {} && partition: {}",result.getProducerRecord().value(),
                        result.getRecordMetadata().offset(),result.getRecordMetadata().partition());
            }
            else{
                log.error("Unable to sent message :{} due to exception :{}",customer,ex.getMessage());
            }
        });
    }

}
