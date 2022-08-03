package org.course.simplekafka.kafka.udemyv3.opensearch;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.network.KafkaChannel;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.lucene.index.IndexReader;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OpensearchConsumer {
    public static RestHighLevelClient createOpenSearchClient() {
        String connString = "https://z49eakztvn:krpayc3a75@kafka-course-9240398805.eu-central-1.bonsaisearch.net:443";

        RestHighLevelClient restHighLevelClient;
        URI connUri = URI.create(connString);

        String userInfo = connUri.getUserInfo();

        if (userInfo == null) {
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));
        } else {
            String[] auth = userInfo.split(":");

            CredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                            .setHttpClientConfigCallback(
                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));

        }
        return restHighLevelClient;
    }


    public static void main(String[] args) throws IOException {
        Logger log = LoggerFactory.getLogger(OpensearchConsumer.class.getSimpleName());

        //create opensearch client

        RestHighLevelClient openSearchClient = createOpenSearchClient();

        //create index if it doesnt exist
        if (!openSearchClient.indices().exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT)) {
            CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
            openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
            log.info("created wikimedia index");
        } else {
            log.info("The wikimedia index already exists");
        }


        // create kafka client
        KafkaConsumer<String, String> kafkaConsumer = createKafkaConsumer();
        kafkaConsumer.subscribe(Collections.singletonList("wikimedia.recentchange"));

        //main code logic
        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(3000));

            int recordCount = records.count();
            log.info("Received {} records", recordCount);
            for (ConsumerRecord<String,String> record: records) {

                IndexRequest indexRequest = new IndexRequest("wikimedia")
                        .source(record.value(), XContentType.JSON);

                IndexResponse response = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);
                log.info(response.getId());
                
            }
        }

    }

    private static KafkaConsumer<String, String> createKafkaConsumer() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "consumer-opensearch-demo");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        return new KafkaConsumer<>(properties);
    }
}
