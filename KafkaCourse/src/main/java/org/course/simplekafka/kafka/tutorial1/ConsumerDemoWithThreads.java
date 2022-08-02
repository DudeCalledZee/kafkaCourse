package org.course.simplekafka.kafka.tutorial1;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoWithThreads {


    public static void main(String[] args) {
        new ConsumerDemoWithThreads().run();
    }

    public void run() {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThreads.class.getName());
        String bootstrapServer = "0.0.0.0:9092";
        String groupId = "my-sixth-application";
        String firstTopic = "first_topic";

        // Latch for deling with multiple threads
        CountDownLatch latch = new CountDownLatch(1);

        // Creat the consumer thread
        logger.info("Creating the consumer thread");
        ConsumerThread myConsumerRunnable = new ConsumerThread(
            firstTopic,
            bootstrapServer,
            groupId,
            latch);

        // Start the thread
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        //Add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
           logger.info("Caught shutdown hook");
            (myConsumerRunnable).shutDown();
            latch.countDown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                logger.error("Application got interrupted", e);
            }
            logger.info("Application has exited");
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted", e);
        } finally {
            logger.info("Application is closing");
        }
    }

    public class ConsumerThread implements Runnable {

        private Logger logger = LoggerFactory.getLogger(ConsumerThread.class.getName());

        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;

        public ConsumerThread(String topic,
                              String bootstrapServer,
                              String groupId,
                              CountDownLatch latch) {
            this.latch = latch;
            // create Consumer properties
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            //create the Consumer
            consumer = new KafkaConsumer<>(properties);

            //Subscribe consumer to our topic(s)
            //Can subscribing to multiple eg, Arrays.asList("first_topic", "second_topic", etc)
            consumer.subscribe(Collections.singleton(topic));
        }

        @Override
        public void run() {
            // poll for new data
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("\n Key: {} \n Value: {} \n Partition: {}\n Offset: {}",
                            record.key(), record.value(), record.partition(), record.offset());
                    }
                }
            } catch (WakeupException e) {
                logger.info("Recieved shutdown signal!");
            } finally {
                consumer.close();
                latch.countDown();
            }
        }

        public void shutDown() {
            //the wakeup() method is a special method to interrupt consumer.poll()
            // it will throw the exception WakeUpException
            consumer.wakeup();
        }

    }

}
