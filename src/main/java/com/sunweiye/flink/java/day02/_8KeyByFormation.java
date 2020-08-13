package com.sunweiye.flink.java.day02;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class _8KeyByFormation {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = lines.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String text, Collector<Tuple2<String, Integer>> collector) throws Exception {
                // 将读取数据切分
                String[] words = text.toLowerCase().split(" ");
                for (String word : words) {
                    collector.collect(Tuple2.of(word, 1));
                }
            }
        });

        // 按照第一个字段进行分类中
        KeyedStream<Tuple2<String, Integer>, Tuple> keybyed = wordAndOne.keyBy(0);

        keybyed.print();

        env.execute("TransFormation");
    }
}
