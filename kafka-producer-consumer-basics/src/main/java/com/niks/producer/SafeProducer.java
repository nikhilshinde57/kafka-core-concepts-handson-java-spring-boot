package com.niks.producer;

import java.util.Properties;
import com.niks.utils.KafkaUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SafeProducer {

  //topic should be follow this naming conventions
  //<data-center>.<domain>.<classification>.<description>.<version>
  static final String TOPIC = "local.niks.kafka.notification.1";
  static final Logger logger = LoggerFactory.getLogger(DefaultProducer.class);

  public static void main(String[] args) {

    try{
      Properties properties = KafkaUtils.getSafeProducerProperties();
      KafkaProducer<String, String> kafkaProducer = KafkaUtils.getProducer(properties);

      logger.info("Producer created.");

      for (int recordCount = 1; recordCount < 11; recordCount++) {

        String value = "Hello Kafka World, I'm a user_" + recordCount;
        String key = "_id" + recordCount;

        ProducerRecord<String, String> recordToProduce = KafkaUtils.getProducerRecordWithKey(TOPIC, value, key);

        logger.info(String.format("Record created with key: %s",key));

        KafkaUtils.produceRecordAsynchronously(kafkaProducer, recordToProduce);

        logger.info(String.format("Record sent to the topic: %s",TOPIC));
        logger.info(String.format("Record value: %s",recordToProduce.value()));
      }

      // flush data
      kafkaProducer.flush();
      // flush and close producer
      kafkaProducer.close();
    }
    catch (Exception ex){
      logger.error("Something went wrong while producing record messages:",ex);
    }
  }
}