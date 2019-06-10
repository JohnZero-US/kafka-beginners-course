package kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * 描述:
 */
/*
  Created by IntelliJ IDEA.
  Type: Class
  User: John Zero
  DateTime: 2019/5/20 17:35
  Description: 
*/
public class ProducerDemoKeys {
    //
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //
        final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

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

        //循环十次
        for (int i = 0; i < 10; i++) {
            String topic = "first_topic";
            String value = "hello world " + i;
            String key = "id_" + i;

            //创建生产者纪录对象
            final ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);
            //发布消息（异步）
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception exception) {//成功发送或者发生异常时调用
                    if (exception == null) {
                        //打印日志
                        logger.info("Received new metadata. \n" +
                                "Topic:" + metadata.topic() + "\n" +
                                "Partition:" + metadata.partition() + "\n" +
                                "Offset:" + metadata.offset() + "\n" +
                                "Timestamp:" + metadata.timestamp() + "\n"
                        );
                    } else {
                        logger.error("Error while producing", exception);
                    }
                }
            }).get(); // 阻止 send方法进行同步，不能在产品中使用
        }
        //刷新并关闭
        producer.flush();
    }
}
