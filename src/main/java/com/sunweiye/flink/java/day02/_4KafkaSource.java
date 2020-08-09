package com.sunweiye.flink.java.day02;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import java.util.Properties;

public class _4KafkaSource {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers","localhost:9092");
        properties.setProperty("group.id","gwc10");
        properties.setProperty("auto.offset.reset","earliest");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("sunday", new SimpleStringSchema(), properties);

        DataStream<String> lines = env.addSource(consumer);

        lines.print();

        env.execute("test");
    }
}
