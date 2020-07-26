package kafka.com.niks;

import java.util.Properties;
import kafka.com.niks.utils.KafkaUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultMessageKeyBasedProducer {

  //topic should be follow this naming conventions
  //<data-center>.<domain>.<classification>.<description>.<version>
  static final String TOPIC = "local.niks.kafka.notification.1";
  static final Logger logger = LoggerFactory.getLogger(DefaultProducer.class);

  public static void main(String[] args) {

    Properties properties = KafkaUtils.getDefaultProducerProperties();
    KafkaProducer<String, String> kafkaProducer = KafkaUtils.getProducer(properties);
    logger.info("Producer created.");

    for (int recordCount = 1; recordCount < 11; recordCount++) {
      String value = "Hello Kafka World, I'm a user_" + recordCount;
      String key = "_id" + recordCount;

      ProducerRecord<String, String> recordToProduce = KafkaUtils.getProducerRecordWithKey(TOPIC, value, key);
      logger.info("Record created.");
      KafkaUtils.produceRecordAsynchronously(kafkaProducer, recordToProduce);
      logger.info("Record sent to the topic: " + TOPIC);
      logger.info("Record value: " + recordToProduce.value());
    }
    // flush data
    kafkaProducer.flush();
    // flush and close producer
    kafkaProducer.close();
  }
}
