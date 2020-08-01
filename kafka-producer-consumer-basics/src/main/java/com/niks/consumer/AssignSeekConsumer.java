package com.niks.consumer;

import java.util.Arrays;
import java.util.Properties;
import com.niks.utils.KafkaUtils;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AssignSeekConsumer {

  //topic should be follow this naming conventions
  //<data-center>.<domain>.<classification>.<description>.<version>
  static final String TOPIC = "local.niks.kafka.notification.1";
  static final Logger logger = LoggerFactory.getLogger(AssignSeekConsumer.class);

  public static void main(String[] args) {
    try {

      int numberOfMessagesToRead = 5;
      int numberOfMessagesReadSoFar = 0;

      //Fetch consumer properties
      Properties consumerProperties = KafkaUtils.getDefaultConsumerProperties();

      //Get kafka consumer
      KafkaConsumer<String, String> kafkaConsumer = KafkaUtils.getConsumer(consumerProperties);
      logger.info("Consumer created.");

      // assign and seek are mostly used to replay data or fetch a specific message
      TopicPartition partitionToReadFrom = new TopicPartition(TOPIC, 0);
      long offsetToReadFrom = 15L;
      kafkaConsumer.assign(Arrays.asList(partitionToReadFrom));
      // seek
      kafkaConsumer.seek(partitionToReadFrom, offsetToReadFrom);
      logger.info(String.format("Consumer subscribed to the topic created %s", TOPIC));

      //Start consumer
      KafkaUtils.startAssignSeekConsumer(kafkaConsumer, numberOfMessagesReadSoFar, numberOfMessagesToRead
      );
    } catch (Exception ex) {
      logger.error("Something went wrong while consuming record messages:", ex);
    }
  }
}
