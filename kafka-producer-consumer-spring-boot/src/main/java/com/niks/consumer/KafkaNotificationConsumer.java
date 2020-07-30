package com.niks.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;

public class KafkaNotificationConsumer {

  @Value("${spring.kafka.topic.name}")
  private String topic;
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaNotificationConsumer.class);

  @KafkaListener(topics = "${spring.kafka.topic.name}")
  public void consume(String message)  {
    LOGGER.info(String.format("Received message from topic : %s", topic));
    LOGGER.info(String.format("Consumed message : %s", message));
    //Further business logic
  }
}
