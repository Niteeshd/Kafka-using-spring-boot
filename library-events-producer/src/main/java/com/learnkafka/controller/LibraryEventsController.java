package com.learnkafka.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.domain.libraryEventType;
import com.learnkafka.producer.LibraryEventsProducer;
import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

@RestController
@Slf4j
public class LibraryEventsController {

    private final LibraryEventsProducer libraryEventsProducer;

    public LibraryEventsController(LibraryEventsProducer libraryEventsProducer) {
        this.libraryEventsProducer = libraryEventsProducer;
    }

    @PostMapping("/v1/libraryevent")
    public ResponseEntity<LibraryEvent> postLibraryEvent(
            @RequestBody  @Valid LibraryEvent libraryEvent
            ) throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {
        log.info("libraryEvent : {} ", libraryEvent);
        /*Here we are invoking the kafka producer.
        When we ge the response body it is being sent to libraryEventsProducer.*/
        //libraryEventsProducer.sendLibraryEvent(libraryEvent);
        //libraryEventsProducer.sendLibraryEvent_approach2(libraryEvent);
        libraryEventsProducer.sendLibraryEvent_approach3(libraryEvent);

        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }



    @PutMapping("/v1/libraryevent")
    public ResponseEntity<?> updateLibraryEvent(
            @RequestBody  @Valid LibraryEvent libraryEvent
    ) throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {

        /* Here we are performing update on event which is already present in the inventory.
         But, we are going to perform some validations coz ID is mandatory in this case.*/

        if(libraryEvent.libraryEventId() == null){

            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("please pass the LibraryEventId");
        }

        if(!libraryEvent.libraryEventType().equals(libraryEventType.UPDATE)){

            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Only Update request is accepted");

        }

        libraryEventsProducer.sendLibraryEvent(libraryEvent);
        //libraryEventsProducer.sendLibraryEvent_approach2(libraryEvent);
        //libraryEventsProducer.sendLibraryEvent_approach3(libraryEvent);

        return ResponseEntity.status(HttpStatus.OK).body(libraryEvent);
    }
}
