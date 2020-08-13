package com.sunweiye.flink.java.day02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import scala.Int;

import java.util.Arrays;

public class _6FlatMapTransFormation {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> input = env.fromElements(1, 2, 3, 4, 5);

        //flatmap 做为拓展函数,输入int返回tuple2,可以将一条数据转为多条数据
        SingleOutputStreamOperator<Tuple2<Integer, Integer>> out = input.flatMap(new FlatMapFunction<Integer, Tuple2<Integer, Integer>>() {
            @Override
            public void flatMap(Integer integer, Collector<Tuple2<Integer, Integer>> collector) throws Exception {
                collector.collect(Tuple2.of(integer, integer * 2));
            }
        });

        // rich函数更牛逼
        SingleOutputStreamOperator<Tuple2<Integer, Integer>> out1 = input.flatMap(new RichFlatMapFunction<Integer, Tuple2<Integer, Integer>>() {
            @Override
            public void flatMap(Integer integer, Collector<Tuple2<Integer, Integer>> collector) throws Exception {
                collector.collect(Tuple2.of(integer, integer * 2));
            }

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }

            @Override
            public void close() throws Exception {
                super.close();
            }
        });

        // sink
        out.print();

        env.execute("_5MapTransFormation");
    }
}
