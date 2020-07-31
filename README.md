# Kafka Complete Core Concepts Hands-on - Java, Spring Boot, ElasticSearch, Kafka Connectors 


This project contains all the [Apache Kafka](http://kafka.apache.org/) core concepts and their use cases implementation.
 
## Goal to Accomplish
#### Understand and Implement
* **Kafka Producer**
  * **Default**
  * **Key based**
  * **Safe**
  * **High Throughput**
* **Kafka Consumer**
   * **Default**
   * **Safe**
   * **Group based**
* **Spring Boot Implementation**
    * **Producer**
    * **Consumer**
    * **Connect**
    * **Streams**
* **Case Studies**
    * **Twitter producer**
    * **Twitter consumer with elastic search as DB**
    * **Kafka Twitter Connect**
    * **Twitter tweets filter using Kafka Streams**
 
## Prerequisites
* **_JDK 8_** - Install JDK >=1.8 version from [here](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)
* **_Apache Kafka_** - Download and install Kafka from [here](https://kafka.apache.org/downloads). [Installation Guide](https://medium.com/@Ankitthakur/apache-kafka-installation-on-mac-using-homebrew-a367cdefd273)
* **_Maven_** - Download latest version of Maven from [here](https://maven.apache.org/download.cgi)Guide
* **_Elastic Search_** - Download and install ElasticSearch from [here](https://www.elastic.co/guide/en/elasticsearch/reference/current/brew.html)
* **_IntelliJ IDEA Community Edition [Optional]_** - Install IntelliJ IDEA Community Edition from [here](https://www.jetbrains.com/idea/#chooseYourEdition)

## Installation
#### Repository
Clone repository source code by executing following instruction to any folder on your machine,
```
https://github.com/nikhilshinde57/kafka-core-concepts-handson-java-spring-boot.git
cd kafka-core-concepts-handson-java-spring-boot
```

## About Projects
The repository contains multiple projects. All the projects are independent of each other, but I will suggest as a beginner it would be great if we run or understand them in the following sequence, that would be really helpful for us to understand kafka concepts steadily.  
Note: Default prerequisites for all projects where your kafka server must be running.
### Projects
* **kafka-producer-consumer-basics**
    * Description: 
        * This project is a set of console applications. Each one implements the different kafka core concepts. By reading the class name you will understand which concept it implements.  
    * Prerequisites:
        * Topic to create: [local.niks.kafka.notification.1]()
    * How to run:
        * From IntelliJ: Just run the respective class's main method
* **kafka-producer-consumer-spring-boot**
    * Description:
        * This project is just a spring boot implementation of producer and consumer. Here mainly we learned how spring boot annotation we can use to implement producer and consumer.
    * Prerequisites:
        * Topic to create: [local.niks.kafka.notification.1]()
    * How to run:
        * From cmd: [mvn spring-boot:run -Dspring-boot.run.profiles=local]() 
        * From IntelliJ: Set the profile variable to local [spring.profiles.active=local]()
* **kafka-usecase-study-twitter-producer**
    * Description:
        * This project mainly focused on use case implementation. 
        This project is really helpful to work on actual massive real time data and to understand all producer's advance configuration settings. Here we can actually deal with advance producer configuration as listed below:
            * Safe/Idempotent producer
            * Avoid data loss using: Producer ACKs
            * Producer retries
            * Prevent messages re-ordering in case of retries using: max.in.flight.requests.per.connection
            * High throughput producer - Use of message compression
            * Add some delay so that producer will wait for that time: linger.ms
            * Batch sizing
    * Prerequisites:
         * Topic to create: [local.niks.kafka.notification.1]()
         * Create and get required keys from Twitter: [How to create a Twitter application](https://docs.inboundnow.com/guide/create-twitter-application/)
         * Get those all keys and put it in application-local.properties file
    * How to run:
         * From cmd: [mvn spring-boot:run -Dspring-boot.run.profiles=local]() 
         * From IntelliJ: Set the profile variable to local. [spring.profiles.active=local ]()
* **kafka-usecase-study-twitter-elasticsearch-consumer**
    * Description:
        * This project mainly focused on use case implementation. 
         This project is really helpful to work on actual massive real time data and to understand all consumer's advanced configuration settings. Here we can actually deal with advance consumer configuration as listed below:
            * Safe consumer: Commit offset explicitly after successful processing of a message only
            * Control number of per request how much data we can consume using : max.poll.records
        * Prerequisites:
            * Topic to create: [local.niks.kafka.notification.1]()
            * Elastic search should be running on your machine
        * How to run:
            * From cmd: [mvn spring-boot:run -Dspring-boot.run.profiles=local]() 
            * From IntelliJ: Set the profile variable to local. [spring.profiles.active=local ]() 
* **kafka-feature-case-study-twitter-connector**
    * Description:
        * Kafka connectors are ready-to-use components. Here we have used a kafka twitter connector and using that by just providing twitter credential we can get the external(Twitter) system data into the kafka topic.
    * Prerequisites:
        * Topic to create: [twitter_status_connect, twitter_deletes_connect]()
        * Create and get required keys from Twitter: [How to create a Twitter application](https://docs.inboundnow.com/guide/create-twitter-application/)
        * Get those all keys and put it in twitter.properties file
    * How to run:
        * Run cmd: [./run.sh]()
* **kafka-feature-case-study-twitter-streams**
    * Description:
        * KafkaStreams enables us to consume data from Kafka topics, analyze or transform data, and potentially,
         send it to another Kafka topic. In this project we fetch the tweets with given #hashtag from twitter. 
         We analyse those tweets, here we are applying logic which extract tweets of those users who are having followers more than 300 and forward it to another topic.
    * Prerequisites:
        * Topic to create: [local.niks.kafka.notification.1, local.niks.twitter.filtered.kafka.notification.1]()
        * Create and get required keys from Twitter: [How to create a Twitter application](https://docs.inboundnow.com/guide/create-twitter-application/)
        * Get those all keys and put it in twitter.properties file
    * How to run:
        * From cmd: [mvn spring-boot:run -Dspring-boot.run.profiles=local]() 
        * From IntelliJ: Set the profile variable to local. [spring.profiles.active=local ]()
        
## Kafka Helpful Commands

 * **Start zookeeper server** : 
    * kafka_2.11-2.4.0/bin/zookeeper-server-start.sh config/zookeeper.properties
 * **Start Kafka server**
    * kafka_2.11-2.4.0/bin/kafka-server-start.sh config/server.properties
 * **Create Topic**
    * kafka_2.11-2.4.0/bin/kafka-topics.sh --zookeeper 127.0.0.1:2181 --topic <TOPIC_NAME> --create --partitions 3 --replication-factor 1
 * **List all created topics**
    * kafka_2.11-2.4.0/bin/kafka-topics.sh --zookeeper 127.0.0.1:2181 --list
 * **Start producer**
    * kafka_2.11-2.4.0/bin/kafka-console-producer.sh --broker-list 127.0.0.1:9092 --topic <TOPIC_NAME>
 * **Start consumer**
    * kafka_2.11-2.4.0/bin/kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic <TOPIC_NAME>
 * **Start consumer for specific group id**
    * kafka_2.11-2.4.0/bin/kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic <TOPIC_NAME> --group <GROUP_NAME>
 * **List all consumer groups**
    * kafka_2.11-2.4.0/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
 * **Describe consumer group**
    * kafka_2.11-2.4.0/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group <GROUP_NAME>
 * **Describe consumer group**
    * kafka_2.11-2.4.0/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group <GROUP_NAME> --rest-offsets --execute --to-earliest --topic <TOPIC_NAME>
 * **Set minimum insync replicas**
    * kafka_2.11-2.4.0/kafka-configs  --zookeeper 127.0.0.1:2181 --entity-type topics --entity-name <TOPIC_NAME> --add-config min.insync.replicas=2 --alter
 
 Last but not the least, Thank you folks for reading this!! I may make mistakes, 
 feel free to point it out so that I can rectify them along the way. And if you find it helpful then don't forget to give a star.
 ##### Happy coding :smile:
