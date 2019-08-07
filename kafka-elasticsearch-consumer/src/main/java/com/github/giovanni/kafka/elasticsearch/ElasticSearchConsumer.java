package com.github.giovanni.kafka.elasticsearch;

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

public class ElasticSearchConsumer {
    final static private Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
    public static RestHighLevelClient createClient(){
         // https://djwjotuheq:y103docoat@kafka-twitter-poc-7452014031.us-west-2.bonsaisearch.net:443
        String hostname = "kafka-twitter-poc-7452014031.us-west-2.bonsaisearch.net";
        String username = "djwjotuheq";
        String password = "y103docoat";

        // don't do if you run a local ES
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,new UsernamePasswordCredentials(username,password));

        RestClientBuilder builder = RestClient.builder(new HttpHost(hostname,443,"https")).setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
            @Override
            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            }
        });

        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }
    public static KafkaConsumer<String,String> createConsumer(String topic){
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "kafka-elasticsearch";


        // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false"); // disable auto commit offsets
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"100");



        // create consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));
        return consumer;
    }
    public static void main(String[] args) {
        RestHighLevelClient client = createClient();

        KafkaConsumer<String,String> consumer = createConsumer("twitter");

        // poll for new data
        while(true){
            ConsumerRecords<String,String> records =
                    consumer.poll(Duration.ofMillis(100));
            int recordCount = records.count();
            logger.info("Received "+recordCount+" records");
            BulkRequest bulkRequest = new BulkRequest();
            for(ConsumerRecord record: records){
                // 2 strategies for creating ID
                // kafka-generic-id
                //String id = record.topic() +"_"+ record.partition() +"_"+ record.offset();
                try {
                    // twitter feed specific id
                    String id = extractIdFromTweet((String) record.value());

                    // where we insert data to ElasticSearch
                    String jsonString = (String) record.value();
                    IndexRequest indexRequest = new IndexRequest("twitter").id(id);
                    indexRequest.source(jsonString, XContentType.JSON);

                    bulkRequest.add(indexRequest);
                }catch(NullPointerException e){
                    logger.warn("Skipping bad data "+ record.value());
                }
            }
            if(recordCount > 0) {
                try {

                    BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                } catch (IOException e) {
                    logger.error("IOException occured while sending a bulk request", e);
                }
                logger.info("Committing offsets...");
                consumer.commitSync();
                logger.info("Offsets have been committed");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    logger.error("Application interrupted", e);
                }
            }
        }
    }
    private static JsonParser jsonParser = new JsonParser();
    private static String extractIdFromTweet(String tweetJson) {
        // use gson library

        return jsonParser.parse(tweetJson)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();

    }
}
