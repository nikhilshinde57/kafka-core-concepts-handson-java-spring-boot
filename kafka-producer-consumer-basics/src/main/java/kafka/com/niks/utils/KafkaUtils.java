package kafka.com.niks.utils;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaUtils {

  public static final String BOOTSTRAP_SERVER = "127.0.0.1:9092";
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
      ProducerRecord recordToProduce) {
    // send data - asynchronous
    kafkaProducer.send(recordToProduce, (recordMetadata, e) -> {
      // executes every time a record is successfully sent or an exception is thrown
      if (e == null) {
        // the record was successfully sent
        logger.info("Received new metadata. \n" +
            "Topic:" + recordMetadata.topic() + "\n" +
            "Partition: " + recordMetadata.partition() + "\n" +
            "Offset: " + recordMetadata.offset() + "\n" +
            "Timestamp: " + recordMetadata.timestamp());
      } else {
        logger.error("Error while producing", e);
      }
    });
  }
}
