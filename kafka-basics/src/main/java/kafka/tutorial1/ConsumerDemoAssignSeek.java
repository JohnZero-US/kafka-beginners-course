package kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * 描述:
 */
/*
  Created by IntelliJ IDEA.
  Type: Class
  User: John Zero
  DateTime: 2019/5/20 18:39
  Description: 
*/
public class ConsumerDemoAssignSeek {

    public static void main(String[] args) {
        //
        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class);
//
        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "my-seven-application";
        String topic = "first_topic";
        //
        Properties properties = new Properties();
        //
        //
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        //
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //
        //创建消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //assign and seek are mostly used to replay data or fetch a specific message

        //指定分区
        TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
        //偏移值
        long offsetToReadFrom = 15L;
        //分配拉取条件
        consumer.assign(Arrays.asList(partitionToReadFrom));
        //
        consumer.seek(partitionToReadFrom, offsetToReadFrom);

        //
        int numberOfMessagesToRead = 5;

        boolean keepOnReading = true;
        //
        int numberOfMessagesReadSoFar = 0;

        //
        while (keepOnReading) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                numberOfMessagesReadSoFar++;
                logger.info("key: " + record.key() + ",value: " + record.value());
                logger.info("partition: " + record.partition() + ",offset: " + record.offset());
                //
                if (numberOfMessagesReadSoFar >= numberOfMessagesToRead) {
                    keepOnReading = false;
                    break;
                }
            }
        }

        logger.info("Exiting the application");

    }
}
