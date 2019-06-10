package kafka.tutorial2;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * 描述:
 */
/*
  Created by IntelliJ IDEA.
  Type: Class
  User: John Zero
  DateTime: 2019/6/1 18:41
  Description: 
*/
public class TwitterProducer {

    //客户端KEY
    String consumerKey = "VMuxvF16xxxxg1LsIYg";
    //客户端密码
    String consumerSecret = "ZlJMxxxxxTWhcGaHGcAU2pxolLWCLeWaE";
    //TOKEN
    String token = "378498185-ilxxxxxxxxwNdailFgKLtHccuTb6ZSwbxgUB";
    //密码
    String secret = "X6P1BsSQ9grM4BzxxxxxxxxEuzTVCx5ipjD01p";
    //主题
    List<String> terms = Lists.newArrayList("bit coin", "usa", "politics", "sport", "soccer");

    /*Consumer key： 	        VMuxvF16xxxxg1LsIYg
    Consumer secret： 	ZlJMxxxxxTWhcGaHGcAU2pxolLWCLeWaE
    Request token URL： 	https://api.twitter.com/oauth/request_token
    Authorize URL：         https://api.twitter.com/oauth/authorize
    Access token URL： 	https://api.twitter.com/oauth/access_token
    Callback URL： 	        http://50.0.0.25:11100/a/HTML/others.br?a=1

    Access token： 	        378498185-ilxxxxxxxxwNdailFgKLtHccuTb6ZSwbxgUB
    Access token secret： 	X6P1BsSQ9grM4BzxxxxxxxxEuzTVCx5ipjD01p*/
    //
    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    //入口函数
    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    //执行
    public void run() {
        //
        logger.info("Setup");
        //Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream
        //信息队列
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
        //创建推特客户端
        Client hosebirdClient = creatTwitterClient(msgQueue);
        // 客户端启动连接
        hosebirdClient.connect();
        //创建kafka提供者
        KafkaProducer<String, String> producer = createKafkaProducer();
        //添加关闭操作
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("stopping application...");
            logger.info("shutting dow client from twitter...");
            hosebirdClient.stop();
            logger.info("closing producer...");
            producer.close();
            logger.info("done!");
        }));

        //循环抓取推特数据到kafka
        // on a different thread, or multiple different threads....
        while (!hosebirdClient.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                hosebirdClient.stop();
            }
            if (msg != null) {
                logger.info(msg);
                //推送消息到kafka broker
                producer.send(new ProducerRecord<>("twitter_tweets", null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception != null) {
                            logger.error("Something bad happened", exception);
                        }
                    }
                });
            }
        }
        //
        logger.info("End of application");
    }


    //创建推特客户端
    public Client creatTwitterClient(BlockingQueue<String> msgQueue) {

        //Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth)
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
        List<Long> followings = Lists.newArrayList(1234L, 566788L);
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        //创建客户端
        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();
        //
        return hosebirdClient;
    }

    //创建kafka提供者
    public KafkaProducer<String, String> createKafkaProducer() {
        //Kafka基础属性
        Properties properties = new Properties();
        //broker
        String bootstrapServers = "127.0.0.1:9092";

        //
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        //键序列化
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //值序列化
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //创建安全的生产者对象的配置
        //启用幂等
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        //确认
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        //重试
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        //最大的预连接，保持此值为5
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        //高吞吐的生产者对象的配置（多线程cpu的使用）
        //压缩类型
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        //徘徊时间(单位：毫秒)
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        //批量提交大小（单位：字节），32KB
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));

        //创建Kafka 生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //
        return producer;
    }
}
