package com.niks.config;

import com.google.gson.JsonParser;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;


@Configuration
@EnableKafka
@EnableKafkaStreams
public class KafkaStreamConfig {

  @Value("${spring.kafka.inputTopic.name}")
  private String inputTopic;
  @Value("${spring.kafka.outputTopic.name}")
  private String outputTopic;
  @Value("${spring.kafka.bootstrap.servers}")
  private String bootstrapServers;
  @Value("${spring.kafka.application.id}")
  private String applicationId;

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaStreamConfig.class);
  private static JsonParser jsonParser = new JsonParser();

  @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
  public KafkaStreamsConfiguration kStreamsConfigs(KafkaProperties kafkaProperties) {
    Map<String, Object> config = consumerConfigs();
    return new KafkaStreamsConfiguration(config);
  }

  @Bean
  public Map<String, Object> consumerConfigs() {

    Map<String, Object> props = new HashMap<>();

    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

    return props;
  }

  @Bean
  public KStream<String, String> kStream(StreamsBuilder kStreamBuilder) {
    KStream<String, String> inputTopicStream = kStreamBuilder.stream(inputTopic);

    KStream<String, String> filteredStream = inputTopicStream.filter(
        // filter for tweets which has a user of over 10000 followers
        (k, jsonTweet) ->  extractUserFollowersInTweet(jsonTweet) > 100
    );
    filteredStream.to(outputTopic);
    return inputTopicStream;
  }

  private static Integer extractUserFollowersInTweet(String tweetJson){
    // gson library
    try {
      return jsonParser.parse(tweetJson)
          .getAsJsonObject()
          .get("user")
          .getAsJsonObject()
          .get("followers_count")
          .getAsInt();
    }
    catch (Exception e){
      LOGGER.info(String.format("Bad data: %s",tweetJson));
      return 0;
    }
  }

}
