package com.atguigu.realtime.util;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class MyKafkaUtil {
    public static FlinkKafkaConsumer<String> getKafkaSource(String groupId,String topic){
        Properties props = new Properties();
        props.setProperty("bootstrap.servers","hadoop162:9092,hadoop163:9092,hadoop163:9092");
        props.setProperty("group.id",groupId);
        props.setProperty("auto.offset.reset","latest");

        return new FlinkKafkaConsumer<>(topic,new SimpleStringSchema(),props);
    }
}
