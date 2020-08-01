package com.niks.producer;

import java.util.Properties;
import com.niks.utils.KafkaUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultProducer {

  //topic should be follow this naming conventions
  //<data-center>.<domain>.<classification>.<description>.<version>
  static final String TOPIC = "local.niks.kafka.notification.1";
  static final Logger logger = LoggerFactory.getLogger(DefaultProducer.class);

  public static void main(String[] args) {

    try {
      //Fetch producer properties
      Properties properties = KafkaUtils.getDefaultProducerProperties();
      //Fetch producer
      KafkaProducer<String, String> kafkaProducer = KafkaUtils.getProducer(properties);
      logger.info("Producer created.");

      ProducerRecord<String, String> recordToProduce = KafkaUtils.getDefaultProducerRecord(TOPIC, "Hello Kafka World!");
      logger.info("Record created.");

      //Start producing records
      KafkaUtils.produceRecordWithoutCallBack(kafkaProducer, recordToProduce);

      logger.info(String.format("Record sent to the topic: %s", TOPIC));
      logger.info(String.format("Record value: %s", recordToProduce.value()));

      // flush data
      kafkaProducer.flush();
      // flush and close producer
      kafkaProducer.close();
      logger.info(String.format("Exiting application."));
    } catch (Exception ex) {
      logger.error("Something went wrong while producing record messages:",ex);
    }
  }
}
