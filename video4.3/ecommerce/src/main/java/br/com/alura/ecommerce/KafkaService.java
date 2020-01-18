package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.function.Consumer;

public class KafkaService<T> implements Closeable {
    private final KafkaConsumer<String, T> consumer;
    private final String topic;
    private final ConsumerFunction<T> function;
    private final Class<T> expectedType;

    public KafkaService(String consumerGroup, String topic, ConsumerFunction<T> function, Class<T> expectedType) {
        this.expectedType = expectedType;
        this.topic = topic;
        this.function = function;
        this.consumer = new KafkaConsumer<>(consumerProperties(consumerGroup));
        consumer.subscribe(Collections.singletonList(topic));
    }


    private Properties consumerProperties(String consumerGroup) {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
        properties.setProperty(GsonDeserializer.TYPE_CONFIG, expectedType.getName());
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        return properties;
    }

    public void run() {
        while (true) {
            var records = consumer.poll(Duration.ofMillis(100));
            if (!records.isEmpty()) {
                System.out.println("Found " + records.count() + " records");
                for (var record : records) {
                    function.parse(record);
                }
            }
        }
    }

    @Override
    public void close() {
        consumer.close();
    }
}
