package com.niks;

import com.niks.producer.KafkaNotificationProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SpringKafkaApplication implements CommandLineRunner {

  @Autowired
  KafkaNotificationProducer kafkaNotificationProducer;

  public static void main(String[] args) {
    SpringApplication.run(SpringKafkaApplication.class, args);
  }

  @Override
  public void run(String... args) throws Exception {
    kafkaNotificationProducer.sendMessage("Hello Kafka World!!!");
  }
}
