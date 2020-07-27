package kafka.com.niks.consumer;

import java.util.Arrays;
import java.util.Properties;
import kafka.com.niks.utils.KafkaUtils;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GroupsReaderConsumer {

  //topic should be follow this naming conventions
  //<data-center>.<domain>.<classification>.<description>.<version>
  static final String TOPIC = "local.niks.kafka.notification.1";
  static final Logger logger = LoggerFactory.getLogger(GroupsReaderConsumer.class);

  public static void main(String[] args) {

    try {
      Properties consumerProperties = KafkaUtils.getConsumerGroupsProperties();

      KafkaConsumer<String, String> kafkaConsumer = KafkaUtils.getConsumer(consumerProperties);

      logger.info("Consumer created.");

      // subscribe consumer to our topic(s)
      kafkaConsumer.subscribe(Arrays.asList(TOPIC));
      logger.info(String.format("Consumer subscribed to the topic created %s", TOPIC));

      KafkaUtils.startConsumer(kafkaConsumer);
    } catch (Exception ex) {
      logger.error("Something went wrong while consuming record messages:", ex);
    }
  }
}
