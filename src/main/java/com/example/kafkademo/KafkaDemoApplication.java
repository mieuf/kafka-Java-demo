package com.example.kafkademo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

@EnableScheduling
@SpringBootApplication
public class KafkaDemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaDemoApplication.class, args);
    }

}
