package com.johnzero.kafka.tutorial3;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
  DateTime: 2019/6/10 17:32
  Description: 
*/
public class ElasticSearchConsumer {

    //创建Rest客户端
    public static RestHighLevelClient createClient() {
        //
        String hostname = "kafka-course-4761885487.us-west-2.bonsaisearch.net";
        String username = "ho6fonsstk";
        String password = "4y55gtklb9";

        //本地ES不要使用此设置
        //创建证书提供者
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        //设置证书提供者
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
        //Rest客户端构建
        RestClientBuilder builder =
                RestClient.builder(new HttpHost(hostname, 443, "https")).setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });
        //
        RestHighLevelClient client = new RestHighLevelClient(builder);
        //
        return client;
    }

    //创建Kafka消费者
    public static KafkaConsumer<String, String> createConsumer(String topic) {
        //
        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "kafka-demo-elasticsearch";
        //
        Properties properties = new Properties();
        //
        //
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        //
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        //自动偏移值设置，最早
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //是否启用自动提交，否
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        //最大下拉纪录，100
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");

        //创建消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //
        consumer.subscribe(Arrays.asList(topic));

        //
        return consumer;
    }

    //启动方法
    public static void main(String[] args) throws IOException {
        //
        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

        //
        RestHighLevelClient client = createClient();


        //绑定主题
        KafkaConsumer<String, String> consumer = createConsumer("twitter_tweets");

        //
        //
        while (true) {
            //下拉记录集合，持续100毫秒
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            //
            Integer recordCount = records.count();

            logger.info("Received" + recordCount + "records");

            //批量请求对象
            BulkRequest bulkRequest = new BulkRequest();

            //循环已下拉的记录集合
            for (ConsumerRecord<String, String> record : records) {
                //
                //生成kafka id
                //String id = record.topic() + "_" + record.partition() + "_" + record.offset();

                try {
                    //从推特中提取id
                    String id = extractIdFromTweet(record.value());

                    //插入数据到ElasticSearch
                    //
                    IndexRequest indexRequest = new IndexRequest("twitter", "tweets",
                            id //制作消费者的idempotent
                    ).source(record.value(), XContentType.JSON);
                    //添加到批量请求
                    bulkRequest.add(indexRequest);
                } catch (NullPointerException e) {
                    logger.warn("skipping bad data: " + record.value());
                }
            }
            //如果记录数量大于0
            if (recordCount > 0) {
                //提交批量请求
                BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                logger.info("Committing offsets...");
                //提交同步
                consumer.commitSync();
                logger.info("Offsets have been committed");

                //下拉延时一秒
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        //
        //client.close();
    }

    //json解析器
    private static JsonParser jsonParser = new JsonParser();

    //从推文中提取id
    private static String extractIdFromTweet(String tweetJson) {
        //
        return jsonParser.parse(tweetJson).getAsJsonObject().get("id_str").getAsString();
    }
}
