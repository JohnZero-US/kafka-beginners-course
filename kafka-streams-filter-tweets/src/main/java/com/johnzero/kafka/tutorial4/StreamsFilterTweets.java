package com.johnzero.kafka.tutorial4;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

/**
 * 描述:
 */
/*
  Created by IntelliJ IDEA.
  Type: Class
  User: John Zero
  DateTime: 2019/6/11 16:02
  Description: 
*/
public class StreamsFilterTweets {

    //
    public static void main(String[] args) {
        //创建属性
        Properties properties = new Properties();
        //
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        //
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafka-streams");
        //键序列化
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        //值序列化
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        //创建拓扑
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        //录入的主题
        KStream<String, String> inputTopic = streamsBuilder.stream("twitter_topics");
        //
        KStream<String, String> filteredStream = inputTopic.filter((k, jsonTweet) ->
                //过滤并获取 订阅数超过10000的用户的推文
                extractUserFollowersFromTweet(jsonTweet) > 10000
        );
        //发布的主题
        filteredStream.to("important_tweets");

        //构建拓扑
        KafkaStreams kafkaStreams = new KafkaStreams(
                streamsBuilder.build(),
                properties
        );

        //启动Streams 程序
        kafkaStreams.start();
    }


    //json解析器
    private static JsonParser jsonParser = new JsonParser();

    //从推文中提取用户的订阅用户数
    private static int extractUserFollowersFromTweet(String tweetJson) {
        try {
            //
            return jsonParser.parse(tweetJson).getAsJsonObject().get("user").getAsJsonObject().get("followers_count").getAsInt();
        } catch (NullPointerException e) {
            return 0;
        }
    }
}
