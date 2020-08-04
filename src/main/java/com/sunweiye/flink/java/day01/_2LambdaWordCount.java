package com.sunweiye.flink.java.day01;

import akka.stream.impl.fusing.Collect;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.zookeeper.org.apache.zookeeper.jute.compiler.JString;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class _2LambdaWordCount {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> lines = env.socketTextStream("localhost", 9999);

        // 使用lambda表达式
        SingleOutputStreamOperator<String> word = lines.flatMap((String  line, Collector<String> out) -> {
            Arrays.stream(line.split(" ")).forEach(out::collect);
        }).returns(Types.STRING);

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = word.map(x -> Tuple2.of(x, 1)).returns(Types.TUPLE(Types.STRING,Types.INT));

        SingleOutputStreamOperator<Tuple2<String, Integer>> sumed = wordAndOne.keyBy(0).sum(1);

        sumed.print();

        env.execute("word");
    }

}
