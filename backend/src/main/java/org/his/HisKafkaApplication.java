package org.his;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.transaction.annotation.EnableTransactionManagement;

/**
 * Hospital Information System (HIS) Apache Kafka Enterprise Event Backbone
 * Java 17 / Spring Boot 3.4 / PostgreSQL
 */
@SpringBootApplication
@EnableTransactionManagement
@EnableScheduling
public class HisKafkaApplication {

    public static void main(String[] args) {
        SpringApplication.run(HisKafkaApplication.class, args);
    }
}
