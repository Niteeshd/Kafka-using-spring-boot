package com.learnkafka.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.LibraryEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
@Slf4j
public class LibraryEventsProducer {

    @Value("${spring.kafka.topic}")
    public String topic;

    private final KafkaTemplate<Integer, String> KafkaTemplate;

    // This is used here to mapping string.
    private final ObjectMapper objectMapper;

    public LibraryEventsProducer(org.springframework.kafka.core.KafkaTemplate<Integer, String> kafkaTemplate, ObjectMapper objectMapper) {
        KafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }


    // this is approach 1 for sending messages to a kafka topic by using kafka template (Asynchronous and recommended).
    public java.util.concurrent.CompletableFuture<SendResult<Integer, String>> sendLibraryEvent(LibraryEvent libraryEvent) throws JsonProcessingException {

        var key = libraryEvent.libraryEventId();
        var value = objectMapper.writeValueAsString(libraryEvent);

        /*Now this call is asynchronous call. So, BTS 2 steps will be happening.
        1. The very first time it make a call it will get the metadata - called block call.
        2. It will just return 201 response irrespective of waiting for message being sent to topic or not.*/

        var completableFuture = KafkaTemplate.send(topic, key, value);

        return completableFuture.whenComplete((sendResult, throwable) -> {

            if(throwable!= null){
                handleFailure(key, value, throwable);

            }else {
                handleSuccess(key, value, sendResult);

            }
        });
    }


    // this is approach 2 for sending messages to a kafka topic (Synchronous).
    public SendResult<Integer, String> sendLibraryEvent_approach2(LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {

        var key = libraryEvent.libraryEventId();
        var value = objectMapper.writeValueAsString(libraryEvent);

        /* Now this becomes a synchronous approach because of which as part of second step it will block
        and wait till the message is sent to the kafka topic
         By doing this synchronous call we are removing the completableFuture call.*/

        // there are 2 variants in get methods as shown below.
        var sendResult =  KafkaTemplate.send(topic, key, value)
               // .get()
                        .get(1, TimeUnit.SECONDS);
        handleSuccess(key,value,sendResult);
        return  sendResult;


    }


    // this is approach 3 for sending messages to a kafka topic by using producer Record.
    public java.util.concurrent.CompletableFuture<SendResult<Integer, String>> sendLibraryEvent_approach3(LibraryEvent libraryEvent) throws JsonProcessingException {

        var key = libraryEvent.libraryEventId();
        var value = objectMapper.writeValueAsString(libraryEvent);

        var producerRecord =  buildProducerRecord(key, value);

        var completableFuture = KafkaTemplate.send(producerRecord);

        return completableFuture.whenComplete((sendResult, throwable) -> {

            if(throwable!= null){
                handleFailure(key, value, throwable);

            }else {
                handleSuccess(key, value, sendResult);

            }
        });
    }

    private ProducerRecord<Integer, String> buildProducerRecord(Integer key, String value) {

        return  new ProducerRecord<>(topic, key,value);
    }

    private void handleSuccess(Integer key, String value, SendResult<Integer, String> sendResult) {

        log.info("message has been sent successfully and the key : {} and value : {} , partition is {} ", key, value, sendResult.getRecordMetadata().partition());
    }

    private void handleFailure(Integer key, String value, Throwable throwable) {
        log.error(" error sending the message and the exception is {} " ,throwable.getMessage(), throwable);
    }
}
