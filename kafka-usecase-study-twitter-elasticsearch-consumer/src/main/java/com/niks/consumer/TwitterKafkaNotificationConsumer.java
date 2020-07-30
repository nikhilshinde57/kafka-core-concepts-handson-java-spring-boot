package com.niks.consumer;

import com.niks.service.ElasticSearchService;
import java.io.IOException;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;

public class TwitterKafkaNotificationConsumer {

  @Autowired
  ElasticSearchService elasticSearchService;

  @Value("${spring.kafka.topic.name}")
  private String topic;
  private static final Logger LOGGER = LoggerFactory.getLogger(TwitterKafkaNotificationConsumer.class);

  //  @KafkaListener(topics = "${spring.kafka.topic.name}", topicPartitions = {
//      @TopicPartition(topic = "${spring.kafka.topic.name}", partitions = {"1"})}, groupId = "group_one")
  @KafkaListener(topics = "${spring.kafka.topic.name}")
  public void consume(List<String> tweets, Acknowledgment acknowledgment,
      @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
      @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) Integer key,
      @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
      @Header(KafkaHeaders.OFFSET) String offset) {

    LOGGER.info(String.format("Received message from topic : %s", topic));
    LOGGER.info(String.format("Number of message received: %d", tweets.size()));
    LOGGER.info(String.format("Messages received: %s", tweets));

    try {
      if (!elasticSearchService.saveTweets(tweets)) {
        LOGGER.info("Committing offsets.");
        acknowledgment.acknowledge();
        LOGGER.info("Offsets have been committed");
      }
    } catch (IOException ex) {
      LOGGER.info("Don't worry our consumer is safe consumer.");
      LOGGER.info("We have't committed our offsets, so we can consume them in next retry.");
      LOGGER.info("Also we set our AUTO_COMMIT_OFFSET setting as false.");
      LOGGER.error("Failed to push tweets in ElasticSearch", ex);
    }
  }
}