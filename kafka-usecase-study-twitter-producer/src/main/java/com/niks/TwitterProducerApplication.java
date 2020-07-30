package com.niks;

import com.niks.producer.TwitterKafkaNotificationProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class TwitterProducerApplication implements CommandLineRunner {

  @Autowired
  TwitterKafkaNotificationProducer twitterKafkaNotificationProducer;

  public static void main(String[] args) {
    SpringApplication.run(TwitterProducerApplication.class, args);
  }

  @Override
  public void run(String... args) throws Exception {
    twitterKafkaNotificationProducer.produceTweets();
  }
}
