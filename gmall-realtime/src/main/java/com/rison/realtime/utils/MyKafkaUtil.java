package com.rison.realtime.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

public class MyKafkaUtil {
    private static final String brokers = "centos101:9092,centos102:9092,centos103:9092";
    public static FlinkKafkaProducer<String> getKafkaProducer(String topic){
        return new FlinkKafkaProducer<String>(
                brokers,
                topic,
                new SimpleStringSchema()
        );
    }
}
