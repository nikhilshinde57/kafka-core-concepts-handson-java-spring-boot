package kafka.com.niks.utils;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaUtils {

  public static final String BOOTSTRAP_SERVER = "127.0.0.1:9092";
  public static final String GROUP_ID = "my-first-application";
  static final Logger logger = LoggerFactory.getLogger(KafkaUtils.class);

  public static Properties getDefaultProducerProperties() {
    //create and set Producer properties
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaUtils.BOOTSTRAP_SERVER);
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    return properties;
  }

  public static Properties getSafeProducerProperties() {
    //create and set Producer properties
    Properties properties = getDefaultProducerProperties();

    //Set Safe producer properties
    //Won't produces duplicate messages on network error
    properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
    //Producer will Wait for leader + replicas acks
    properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
    properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
    //Prevent messages re-ordering in case of retires
    //If kafka 2.0 >= 1.1 if yes set to 5 else  set to 1 otherwise.
    properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

    return properties;

    //Note: For Safe producer don't forgot to apply
    //min.insync.replicas configuration at broker/topic level
  }

  public static Properties getSafeHighThroughputProducerProperties() {
    //create and set Producer properties
    Properties properties = getSafeProducerProperties();

    //High throughput producer
    //0: No compression, 1: GZIP compression, 2: Snappy compression, 3: LZ4 compression
    properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
    //If incoming record rate is higher that the record sent rate
    //Add some delay so that producer will wait for that time and then it will send
    properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
    // 32 KB batch size
    properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));

    return properties;
  }

  public static KafkaProducer<String, String> getProducer(Properties properties) {
    //Create Producer
    return new KafkaProducer<String, String>(properties);
  }

  public static ProducerRecord getDefaultProducerRecord(String topic, String message) {
    // create a producer record
    return new ProducerRecord<String, String>(topic, message);
  }

  public static ProducerRecord getProducerRecordWithKey(String topic, String message, String key) {
    //create a producer record with Key
    //When key is sent the, then all messages for that key will always goes to same partition
    return new ProducerRecord<String, String>(topic, key, message);
  }

  public static void produceRecordWithoutCallBack(KafkaProducer<String, String> kafkaProducer,
      ProducerRecord recordToProduce) {
    // send data - asynchronous
    kafkaProducer.send(recordToProduce);
  }

  public static void produceRecordAsynchronously(KafkaProducer<String, String> kafkaProducer,
      ProducerRecord recordToProduce) throws ExecutionException, InterruptedException {
    // send data - asynchronous
    kafkaProducer.send(recordToProduce, (recordMetadata, e) -> {
      // executes every time a record is successfully sent or an exception is thrown
      if (e == null) {
        // the record was successfully sent
        logger.info("Record published with key: "+recordToProduce.key());
        logger.info("Received new metadata. \n" +
            "Topic:" + recordMetadata.topic() + "\n" +
            "Partition: " + recordMetadata.partition() + "\n" +
            "Offset: " + recordMetadata.offset() + "\n" +
            "Timestamp: " + recordMetadata.timestamp());
      } else {
        logger.error("Error while producing", e);
      }
    }).get();// Don't do this production this is just for to verify that the message with same _id is always goes to same partition
  }

  public static Properties getDefaultConsumerProperties() {
    //create and set Consumer properties
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaUtils.BOOTSTRAP_SERVER);
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    return properties;
  }

  public static Properties getConsumerGroupsProperties() {
    //create and set Consumer properties
    Properties properties = getDefaultConsumerProperties();
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
    return properties;
  }

  public static Properties geSafeConsumerProperties() {
    //create and set Consumer properties
    Properties properties = getConsumerGroupsProperties();
    // disable auto commit of offsets
    properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    // disable auto commit of offsets
    properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
    return properties;
  }

  public static KafkaConsumer<String, String> getConsumer(Properties properties) {
    //Create Producer
    return new KafkaConsumer<String, String>(properties);
  }

  public static void startConsumer(KafkaConsumer<String, String> kafkaConsumer) {

    logger.info("Consumer started.");
    // poll for new data
    while (true) {

      ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100)); // new in Kafka 2.0.0

      for (ConsumerRecord<String, String> record : records) {
        logger.info("Key: " + record.key() + ", Value: " + record.value());
        logger.info("Partition: " + record.partition() + ", Offset:" + record.offset());
      }
    }
  }

  public static void startAssignSeekConsumer(KafkaConsumer<String, String> kafkaConsumer, int numberOfMessagesReadSoFar,
      int numberOfMessagesToRead) {

    logger.info("Consumer started.");
    boolean keepOnReading = true;
    // poll for new data
    while (keepOnReading) {
      ConsumerRecords<String, String> records =
          kafkaConsumer.poll(Duration.ofMillis(100)); // new in Kafka 2.0.0

      for (ConsumerRecord<String, String> record : records) {
        numberOfMessagesReadSoFar += 1;
        logger.info("Key: " + record.key() + ", Value: " + record.value());
        logger.info("Partition: " + record.partition() + ", Offset:" + record.offset());
        if (numberOfMessagesReadSoFar >= numberOfMessagesToRead) {
          keepOnReading = false; // to exit the while loop
          break; // to exit the for loop
        }
      }
    }
    logger.info("Exiting the application");
  }

  public static void startSafeConsumer(KafkaConsumer<String, String> kafkaConsumer) {

    logger.info("Consumer started.");
    // poll for new data
    while (true) {

      ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100)); // new in Kafka 2.0.0

      for (ConsumerRecord<String, String> record : records) {
        //Do some business logic
        logger.info("Key: " + record.key() + ", Value: " + record.value());
        logger.info("Partition: " + record.partition() + ", Offset:" + record.offset());
      }
      //After successful processing of message commit offset
      //So that in case anything goes wrong while processing message then we can re-read the message from kafka
      logger.info("Committing offsets...");
      kafkaConsumer.commitSync();
      logger.info("Offsets have been committed");
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }
}
