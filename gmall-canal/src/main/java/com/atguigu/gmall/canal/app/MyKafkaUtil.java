package com.atguigu.gmall.canal.app;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Map;

public class MyKafkaUtil {

    private static KafkaProducer<String,String> kafkaProducer;

    private MyKafkaUtil() {
    }

    private static KafkaProducer<String,String> createProducer() {
        Map<String,Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092");
        kafkaParams.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaParams.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return new KafkaProducer<String,String>(kafkaParams);
    }

    public static void sendMessage(String topic, String message) {
        synchronized (MyKafkaUtil.class) {
            if (kafkaProducer == null) {
                kafkaProducer = createProducer();
            }
        }
        kafkaProducer.send(new ProducerRecord<String,String>(topic,message));
    }


}
