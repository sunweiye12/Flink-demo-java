package com.sunweiye.flink.java.day02;

import jdk.nashorn.internal.codegen.types.Type;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class _7FlierTransFormation {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> input = env.fromElements(1, 2, 3, 4, 5);

        //fliter最为一个过滤函数在使用(当输入值进入函数返回为true时才能通过)
        SingleOutputStreamOperator<Integer> ood = input.filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer i) throws Exception {
                return i % 2 == 0;
            }
        });

        // lambda函数实现
        SingleOutputStreamOperator<Integer> ood1 = input.filter(x -> x % 2 == 0).returns(Types.INT);

        // sink
        ood1.print();

        env.execute("_5MapTransFormation");
    }
}
