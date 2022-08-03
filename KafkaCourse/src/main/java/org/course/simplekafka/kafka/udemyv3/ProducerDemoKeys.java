package org.course.simplekafka.kafka.udemyv3;


import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class.getSimpleName());

    public static void main(String[] args) {
        //create properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

//        create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

//        send the data
//        flush and close producer
        for (int i = 0; i < 20; i++) {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "id_"+i,"Hello Humans " + i );

            producer.send(producerRecord, (recordMetadata, e) -> {
                if (e == null) {
                    log.info("" +
                            "Recieved new MetaData \n" +
                            "Topic: {} \n" +
                            "Key: {} \n" +
                            "Partition: {} \n" +
                            "Offset: {} \n" +
                            "Timestamp: {}", recordMetadata.topic(), producerRecord.key(), recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp());
                } else {
                    log.error("An Error was thrown", e);
                }
            });
        }
        producer.flush();
        producer.close();
    }
}
