package com.niks.producer;

import com.niks.config.TwitterConfig;
import com.twitter.hbc.core.Client;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
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
public class TwitterKafkaNotificationProducer {

  private static final Logger LOGGER = LoggerFactory.getLogger(TwitterKafkaNotificationProducer.class);

  @Value("${spring.kafka.topic.name}")
  private String topic;
  @Value("${spring.kafka.bootstrap.servers}")
  private String bootstrapServers;
  @Autowired
  private KafkaTemplate<String, String> kafkaTemplate;
  @Autowired
  private TwitterConfig twitterConfig;

  public void sendMessage(String payload) {

    try {
      LOGGER.info(String.format("Sending message to the topic: %s with payload %s", topic, payload));
      ListenableFuture<SendResult<String, String>> messageSentResponse = this.kafkaTemplate.send(topic, payload);

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
    } catch (Exception ex) {
      LOGGER.error("Error occurred while producing tweet", ex);
    }
  }

  public void produceTweets() {

    BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(100000);
    Client twitterClient = twitterConfig.getTwitterClient(msgQueue);
    twitterClient.connect();

    while (!twitterClient.isDone()) {
      String receivedTweet = null;
      try {
        receivedTweet = msgQueue.poll(5, TimeUnit.SECONDS);
      } catch (Exception ex) {
        LOGGER.error("Error occurred while fetching tweets from twitter", ex);
        twitterClient.stop();
      }
      if (receivedTweet != null) {
        LOGGER.info(String.format("Received tweet: %s", receivedTweet));
        sendMessage(receivedTweet);
      }
    }

    // add a shutdown hook
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      LOGGER.info("Stopping twitter connection...");
      LOGGER.info("Shutting down client from twitter...");
      twitterClient.stop();
      LOGGER.info("Done!");
    }));

  }
}
