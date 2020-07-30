package com.niks.config;

import com.niks.producer.TwitterKafkaNotificationProducer;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

@Configuration
public class ProducerConfig {

  @Value("${spring.kafka.bootstrap.servers}")
  private String bootstrapServers;

  @Bean
  public Map<String, Object> producerConfigs() {
    Map<String, Object> props = new HashMap<>();
    // list of host:port pairs used for establishing the initial connections to the Kakfa cluster
    props.put(org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
        bootstrapServers);
    props.put(org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        StringSerializer.class);
    props.put(org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        StringSerializer.class);

    //Set Safe producer properties
    //Won't produces duplicate messages on network error
    props.put(org.apache.kafka.clients.producer.ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
    //Producer will Wait for leader + replicas acks
    props.put(org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG, "all");
    props.put(org.apache.kafka.clients.producer.ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
    //Prevent messages re-ordering in case of retires
    //If kafka 2.0 >= 1.1 if yes set to 5 else  set to 1 otherwise.
    props.put(org.apache.kafka.clients.producer.ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");


    //High throughput producer
    //0: No compression, 1: GZIP compression, 2: Snappy compression, 3: LZ4 compression
    props.put(org.apache.kafka.clients.producer.ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
    //If incoming record rate is higher that the record sent rate
    //Add some delay so that producer will wait for that time and then it will send
    props.put(org.apache.kafka.clients.producer.ProducerConfig.LINGER_MS_CONFIG, "20");
    // 32 KB batch size
    props.put(org.apache.kafka.clients.producer.ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));

    return props;
  }

  @Bean
  public ProducerFactory<String, String> producerFactory() {
    return new DefaultKafkaProducerFactory<>(producerConfigs());
  }

  @Bean
  public KafkaTemplate<String, String> kafkaTemplate() {
    return new KafkaTemplate<>(producerFactory());
  }

  @Bean
  public TwitterKafkaNotificationProducer sender() {
    return new TwitterKafkaNotificationProducer();
  }
}
