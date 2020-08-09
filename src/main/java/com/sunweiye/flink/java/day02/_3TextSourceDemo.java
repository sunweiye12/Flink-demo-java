package com.sunweiye.flink.java.day02;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.NumberSequenceIterator;


public class _3TextSourceDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String input = "file:///Users/bytedance/test.txt";
        DataStreamSource<String> text = env.readTextFile(input).setParallelism(5);
        // 多并行的sourceAPI 默认是本地设置的并行度
        System.out.println("-----"+text.getParallelism());

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String text, Collector<Tuple2<String, Integer>> collector) throws Exception {
                // 将读取的数据按照分隔符来进行切割
                String[] tokens = text.toLowerCase().split("\t");
                // 将每一个单词形成一个Tuple加上他们的次数 1
                for (String token : tokens) {
                    collector.collect(new Tuple2<String, Integer>(token, 1));
                }
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> sumed = wordAndOne.keyBy(0).sum(1).setParallelism(3);
        System.out.println("-----"+sumed.getParallelism());

        // sink
        sumed.print();
        env.execute("demo");
    }
}
