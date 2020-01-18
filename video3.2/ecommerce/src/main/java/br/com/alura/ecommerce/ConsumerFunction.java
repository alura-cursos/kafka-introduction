package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface ConsumerFunction {

    void parse(ConsumerRecord<String, String> record);
}
