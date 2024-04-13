package com.learnkafka.domain;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public record LibraryEvent(
        Integer libraryEventId,
        libraryEventType  libraryEventType,
        @NotNull
        @Valid
        Book book
) {
}
