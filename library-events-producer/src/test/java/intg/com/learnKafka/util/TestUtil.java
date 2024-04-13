package com.learnKafka.util;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.Book;
import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.domain.libraryEventType;

public class TestUtil {

    public static LibraryEvent libraryEventRecord(){

        return new LibraryEvent(null,
                libraryEventType.NEW,
                bookRecord());
    }

    public static LibraryEvent libraryEventRecordUpdate(){

        return new LibraryEvent(1,
                libraryEventType.UPDATE,
                bookRecordUpdate());
    }

    private static Book bookRecordUpdate() {

        return new Book(456, " Update Kafka using spring boot", "Niteesh");
    }


    private static Book bookRecord() {

        return new Book(123, "Kafka using spring boot", "Niteesh");
    }


    public static LibraryEvent parseLibraryEventRecord(ObjectMapper objectMapper, String json){
        try{
            return objectMapper.readValue(json, LibraryEvent.class);

        }catch (JsonProcessingException e){
            throw new RuntimeException(e);
        }

    }

    public static Object libraryEventRecordInvalidValues() {

        return new LibraryEvent(null,
                libraryEventType.NEW,
                bookRecordWithInvalidValues());
    }

    private static Book bookRecordWithInvalidValues() {

        return new Book(null,"", "Niteesh");
    }
}
