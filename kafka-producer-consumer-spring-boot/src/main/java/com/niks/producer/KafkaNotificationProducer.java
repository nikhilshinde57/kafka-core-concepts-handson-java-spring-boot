package com.niks.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

@Service
public class KafkaNotificationProducer {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaNotificationProducer.class);

  @Value("${spring.kafka.topic.name}")
  private String topic;
  @Value("${spring.kafka.bootstrap.servers}")
  private String bootstrapServers;
  @Autowired
  private KafkaTemplate<String, String> kafkaTemplate;


  public void sendMessage(String payload) {

    LOGGER.info(String.format("Sending message to the topic: %s with payload %s", topic,payload));
    ListenableFuture<SendResult<String, String>> messageSentResponse= this.kafkaTemplate .send(topic, payload);

    messageSentResponse.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {

      @Override
      public void onSuccess(SendResult<String, String> result) {
        LOGGER.info("Sent message=[" + payload +
            "] with offset=[" + result.getRecordMetadata().offset() + "]");
      }
      @Override
      public void onFailure(Throwable ex) {
        LOGGER.info("Unable to send message=["
            + payload + "] due to : " + ex.getMessage());
      }
    });
  }
}
