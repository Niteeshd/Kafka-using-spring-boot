package com.learnKafka.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnKafka.util.TestUtil;
import com.learnkafka.controller.LibraryEventsController;
import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.producer.LibraryEventsProducer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;


//Test Slice - we are slicing the application and just using entry point layer or web layer.

@WebMvcTest(LibraryEventsController.class)
public class LibraryEventsControllerUnitTest {

    // using mockMVC we can invoke the endpoints (perform the REST calls).
    @Autowired
    MockMvc mockMvc;

    @Autowired
    ObjectMapper objectMapper;

    //Injecting the dependency over here.
    @MockBean
    LibraryEventsProducer libraryEventsProducer;



    @Test
    void postLibraryEvent() throws Exception {

        // leveraging when method which is part of Mockito

        when(libraryEventsProducer.sendLibraryEvent(isA(LibraryEvent.class)))
                .thenReturn(null);

        var json= objectMapper.writeValueAsString(TestUtil.libraryEventRecord());
        mockMvc.perform(MockMvcRequestBuilders.post("/v1/libraryevent")
                .content(json)
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isCreated());


    }


    @Test
    void postLibraryEvent_invalidValues() throws Exception {

        // leveraging when method which is part of Mockito

        when(libraryEventsProducer.sendLibraryEvent(isA(LibraryEvent.class)))
                .thenReturn(null);

        var json= objectMapper.writeValueAsString(TestUtil.libraryEventRecordInvalidValues());
        mockMvc.perform(MockMvcRequestBuilders.post("/v1/libraryevent")
                        .content(json)
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().is4xxClientError());


    }
}


