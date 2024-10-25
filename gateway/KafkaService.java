package it.polimi.gateway;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaService {
    private final KafkaProducer<String, String> producer;
    private final KafkaConsumer<String, String> consumer;

    // map to keep track of requests
    private final ConcurrentHashMap<String, CompletableFuture<String>> pendingRequests = new ConcurrentHashMap<>();    
    private final Properties properties = new Properties();
    private int pollTimeout;

    public KafkaService() {
        try {
            FileInputStream input = new FileInputStream("config.properties");
            properties.load(input);
        } catch (IOException e) {
            Logger.Log("Error reasing properties file");
            e.printStackTrace();
        }

        pollTimeout = Integer.parseInt(properties.getProperty("kafka.polltimeout", "100"));
    
        Properties producerProperties = new Properties();
        producerProperties.put("bootstrap.servers", properties.getProperty("kafka.servers", "localhost:9092"));
        producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        this.producer = new KafkaProducer<>(producerProperties);

        Properties consumerProperties = new Properties();
        consumerProperties.put("bootstrap.servers", properties.getProperty("kafka.servers", "localhost:9092"));
        consumerProperties.put("group.id", properties.getProperty("kafka.consumergroupid", "consumer_group"));
        consumerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        this.consumer = new KafkaConsumer<>(consumerProperties);
        this.consumer.subscribe(Collections.singletonList(properties.getProperty("kafka.replytopic", "reply_topic")));

        new Thread(this::consumeResponses).start();
    }

    public CompletableFuture<String> publishEvent(String requestId, String topic, String data) {
        CompletableFuture<String> responseFuture = new CompletableFuture<>();
        pendingRequests.put(requestId, responseFuture);
        producer.send(new ProducerRecord<>(topic, requestId, data));

        return responseFuture;
    }

    private void consumeResponses() {
        while (true) {
            consumer.poll(java.time.Duration.ofMillis(pollTimeout))
                .forEach(this::handleResponse);
                //.forEach(record -> Logger.Log(record.value() + " " + record.key()));
        }
    }
    

    private void handleResponse(ConsumerRecord<String, String> event) {
        String requestId = event.key();
        String data = event.value();

        Logger.Log("Received event: " +
                   "Key: " + event.key() + ", " +
                   "Value: " + event.value() + ", " +
                   "Partition: " + event.partition() + ", " +
                   "Offset: " + event.offset() + ", " +
                   "Timestamp: " + event.timestamp());

        CompletableFuture<String> responseFuture = pendingRequests.remove(requestId);
        if (responseFuture != null) {
            responseFuture.complete(data);
        } else {
            Logger.Log("Received response for unknown requestId " + requestId);
        }
    }
}
