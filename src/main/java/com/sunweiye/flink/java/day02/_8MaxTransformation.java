package com.sunweiye.flink.java.day02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class _8MaxTransformation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> lines = env.socketTextStream("localhost",9999);

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndNum = lines.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                String[] val = s.split(",");
                String key = val[0];
                int num = Integer.parseInt(val[1]);
                return Tuple2.of(key,num);
            }
        });

        KeyedStream<Tuple2<String, Integer>, Tuple> keyed = wordAndNum.keyBy(0);

        // 获取分组后的做大值(每一次输入都会输出这组的最大值)
        SingleOutputStreamOperator<Tuple2<String, Integer>> maxed = keyed.max(1);

        maxed.print();

        // 5 启动程序
        env.execute("WordCunt");
    }
}
