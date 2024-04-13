package com.learnkafka.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class AutoCreateConfig {

    // This value is being taken from yml file.
    @Value("${spring.kafka.topic}")
    public String topic;
    @Bean
    public NewTopic libraryEvents(){

        // here we are making use of utility class topic builder
        return TopicBuilder
                .name(topic)
                .partitions(3)
                .replicas(3)
                .build();

    }
}
