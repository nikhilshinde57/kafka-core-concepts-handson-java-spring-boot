package com.niks.consumer;

import java.util.Arrays;
import java.util.Properties;
import com.niks.utils.KafkaUtils;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultConsumer {

  //topic should be follow this naming conventions
  //<data-center>.<domain>.<classification>.<description>.<version>
  static final String TOPIC = "local.niks.kafka.notification.1";
  static final Logger logger = LoggerFactory.getLogger(DefaultConsumer.class);

  public static void main(String[] args) {

    try {
      //Fetch consumer properties
      Properties consumerProperties = KafkaUtils.getConsumerGroupsProperties();

      //Get kafka consumer
      KafkaConsumer<String, String> kafkaConsumer = KafkaUtils.getConsumer(consumerProperties);
      logger.info("Consumer created.");

      // subscribe consumer to our topic(s)
      kafkaConsumer.subscribe(Arrays.asList(TOPIC));
      logger.info(String.format("Consumer subscribed to the topic created %s", TOPIC));

      //Start consumer
      KafkaUtils.startConsumer(kafkaConsumer);
    } catch (Exception ex) {
      logger.error("Something went wrong while consuming record messages:", ex);
    }
  }
}
