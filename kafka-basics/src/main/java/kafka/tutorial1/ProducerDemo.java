package kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * 描述:
 */
/*
  Created by IntelliJ IDEA.
  Type: Class
  User: John Zero
  DateTime: 2019/5/20 17:17
  Description: 
*/
public class ProducerDemo {

    //
    public static void main(String[] args) {
        //
        Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallBack.class);

        //Kafka基础属性
        Properties properties = new Properties();
        //
        String bootstrapServers = "127.0.0.1:9092";
        //
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        //
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //创建Kafka 生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //创建生产者纪录对象
        final ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "hello world");

        //发布消息（异步）
        producer.send(record);

        //刷新并关闭
        producer.flush();
    }
}
