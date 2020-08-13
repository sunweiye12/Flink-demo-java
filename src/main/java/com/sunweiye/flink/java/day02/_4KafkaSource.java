package com.sunweiye.flink.java.day02;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import java.util.Properties;

public class _4KafkaSource {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties props = new Properties();
        props.put("bootstrap.servers", "127.0.0.1:9092");
        props.put("group.id", "Test");
        props.put("enable.auto.commit", true);//设置是否为自动提交
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>(
                "test",
                new SimpleStringSchema(),
                props);

        DataStream<String> lines = env.addSource(consumer);

        //sink
        lines.print();

        env.execute("test");
    }
}
