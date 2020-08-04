package com.sunweiye.flink.java.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 这个程序是上一个程序的简化版本
 */
public class _1WordCount {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> lines = env.socketTextStream("localhost",9999);

        // 将上面两部化简成一步 (将flatmap和map两部做一下结合,输入一个字符串将其切割后,返回出对应的map结构)
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

        // 3.核心操作3 以第一个字段进行分组,将第二个字段进行相加
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumned = wordAndOne.keyBy(0).sum(1);

        // 4 操作结束,输出结果(Sink调用)
        sumned.print();

        // 5 启动程序
        env.execute("WordCunt");
    }
}
