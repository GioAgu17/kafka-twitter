package com.github.giovanni.kafka.twitter;

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
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {
    private Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());
    private List<String> terms = Lists.newArrayList("bitcoin","trump","china","sport","soccer");

    private TwitterProducer(){

    }
    public static void main(String[] args) {
        new TwitterProducer().run();
    }
    private void run(){
        logger.info("Setup");
        // create a twitter client
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
        Client client = createTwitterClient(msgQueue);
        // attempt to establish a connection
        client.connect();

        // create a kafka producer
        KafkaProducer<String,String> producer = createKafkaProducer();

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("stopping application...");
            logger.info("shutting down client from Twitter...");
            client.stop();
            logger.info("closing producer...");
            producer.close();
            logger.info("done!");
        }));
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                logger.error("Application is interrupted", e);
                client.stop();
            }
            if(msg!=null)
                logger.info(msg);
                producer.send(new ProducerRecord<String, String>("twitter_tweets", null, msg), new Callback() {
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(e!=null){
                            logger.error("Something bad happened",e);
                        }
                    }
                });
        }
        logger.info("End of application");



        // loop to send tweets to kafka
    }

    private KafkaProducer<String, String> createKafkaProducer() {
            String bootstrapServers = "127.0.0.1:9092";

            //create Producer properties
            Properties properties = new Properties();
            properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

            // create safer producer
            properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
            properties.setProperty(ProducerConfig.ACKS_CONFIG,"all");
            properties.setProperty(ProducerConfig.RETRIES_CONFIG,Integer.toString(Integer.MAX_VALUE));
            properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,"5"); // kafka 2.3 >= kafka 1.1 so we can keep this as 5. Otherwise use 1.

            // high throughput producer (at the expense of a bit of latency and CPU usage)
            properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");
            properties.setProperty(ProducerConfig.LINGER_MS_CONFIG,"20");
            properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,Integer.toString(32*1024));

            // create the Producer
            KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

            return producer;
    }

    private Client createTwitterClient(BlockingQueue<String> msgQueue){


        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        // setup terms
        hosebirdEndpoint.trackTerms(terms);

        Config config = ConfigFactory.load("twitter.conf");
        String consumerKey = config.getString("twitter.consumer.key");
        String consumerSecret = config.getString("twitter.consumer.secret");
        String accessToken = config.getString("twitter.access.token");
        String accessSecret = config.getString("twitter.access.secret");

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, accessToken, accessSecret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();

        return hosebirdClient;

    }
}
