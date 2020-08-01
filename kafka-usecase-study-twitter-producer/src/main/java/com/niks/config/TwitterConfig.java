package com.niks.config;

import com.google.common.collect.Lists;
import com.niks.config.exception.TwitterConnectionException;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
public class TwitterConfig {

  private static final Logger LOGGER = LoggerFactory.getLogger(TwitterConfig.class);
  @Value("${spring.kafka.twitter.consumer.key}")
  private String consumerKey;
  @Value("${spring.kafka.twitter.consumer.secret}")
  private String consumerSecret;
  @Value("${spring.kafka.twitter.token}")
  private String token;
  @Value("${spring.kafka.twitter.secret}")
  private String secret;
  @Value("${spring.kafka.twitter.hashtag}")
  private String twitterHashTag;

  public Client getTwitterClient(BlockingQueue<String> msgQueue) {

    try {
      /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or
       * oauth) */
      Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
      StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

      //Terms which you wants to get in tweet
      List<String> terms = Lists.newArrayList(twitterHashTag);
      hosebirdEndpoint.trackTerms(terms);

      Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);
      ClientBuilder builder = new ClientBuilder()
          .name("Hosebird-Client-01")     // optional: mainly for the logs
          .hosts(hosebirdHosts)
          .authentication(hosebirdAuth)
          .endpoint(hosebirdEndpoint)
          .processor(new StringDelimitedProcessor(msgQueue));
      // optional: use this if you want to process client events

      return builder.build();
    } catch (Exception ex) {
      LOGGER.error(ex.getMessage(),ex);
      throw  new TwitterConnectionException("Failed to connect twitter application");
    }
  }
}
