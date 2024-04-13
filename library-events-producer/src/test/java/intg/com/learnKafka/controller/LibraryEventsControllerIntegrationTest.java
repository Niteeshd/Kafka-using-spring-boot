package com.learnKafka.controller;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnKafka.util.TestUtil;
import com.learnkafka.domain.LibraryEvent;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.*;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;

/* Here to use embedded kafka broker and override our bootstrap servers we are using 2 annotations
   @EmbeddedKafka and @TestPropertySource*/

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(topics = "library-events")
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.admin.properties.bootstrap.servers=${spring.embedded.kafka.brokers}"})
public class LibraryEventsControllerIntegrationTest {


    @Autowired
    TestRestTemplate restTemplate;

    @Autowired
    ObjectMapper objectMapper;

    @Test
    void postLibraryEvent(){

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.set("content-type", MediaType.APPLICATION_JSON.toString());

        var httpEntity = new HttpEntity<>(TestUtil.libraryEventRecord());

        var responseEntity = restTemplate.exchange("/v1/libraryevent", HttpMethod.POST, httpEntity, LibraryEvent.class);

          assertEquals(HttpStatus.CREATED, responseEntity.getStatusCode());


          ConsumerRecords<Integer, String> consumerRecords= KafkaTestUtils.getRecords(consumer);

          assert consumerRecords.count() ==1;

          consumerRecords.forEach( record ->{
              var libraryEventActual = TestUtil.parseLibraryEventRecord(objectMapper, record.value());
              System.out.println("libraryEventActual :" + libraryEventActual);
              assertEquals(libraryEventActual, TestUtil.libraryEventRecord());

          });

    }




    @Test
    void updateLibraryEvent(){

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.set("content-type", MediaType.APPLICATION_JSON.toString());

        var httpEntity = new HttpEntity<>(TestUtil.libraryEventRecordUpdate());

        var responseEntity = restTemplate.exchange("/v1/libraryevent", HttpMethod.PUT, httpEntity, LibraryEvent.class);

        assertEquals(HttpStatus.OK, responseEntity.getStatusCode());


    }


    /* Now we are going to instantiate a kafka consumer and wire it with embedded kafka broker and consume message*/

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    private Consumer<Integer, String> consumer;

    /*  we are Instantiating a new consumer before every test run by using setUp method.
    Here we are making use of a handy utility class DefaultKafkaConsumer to create a consumer*/

    @BeforeEach
    void setUp() {

        var configs = new HashMap<>(KafkaTestUtils.consumerProps("group1", "true", embeddedKafkaBroker));
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        consumer = new DefaultKafkaConsumerFactory<>(configs, new IntegerDeserializer(), new StringDeserializer())
                .createConsumer();
        embeddedKafkaBroker.consumeFromAllEmbeddedTopics(consumer);
    }


    /* We are setting up tear down function to close consumer once test run is done*/

    @AfterEach
    void tearDown() {
        consumer.close();
    }
}
