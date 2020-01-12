package com.sunweiye.flink.java.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 使用 java 来开发一个流式的开发应用数据程序
 */
public class StreamingWCJavaApp {

    public static void main(String[] args) throws Exception {

        // 1.获取上下文
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2.通过 socket 获取实时的数据流(读取本地 9999 端口 socket 的数据)
        DataStreamSource<String> source = env.socketTextStream("localhost",9999);

        // 3.核心操作
        source.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void flatMap(String text, Collector<Tuple2<String, Integer>> collector) throws Exception {
                // 将读取的数据逗号进行切割
                String[] tokens = text.toLowerCase().split(",");
                // 将每一个单词形成一个Tuple加上他们的次数1
                for (String token:tokens) {
                    if (token.length() > 0) {
                        collector.collect(new Tuple2<String,Integer>(token,1));
                    }
                }
            }
        }).keyBy(0)                     // 在批处理中是 groupBy 在流处理中是 keyBy
                .timeWindow(Time.seconds(5))    // 每隔 5 秒执行一次
                .sum(1)
                .setParallelism(1)              // 设置并行度为 1
                .print();

        // 流式开发不许要执行环境
        env.execute("StreamingWCJavaApp");
    }
}
