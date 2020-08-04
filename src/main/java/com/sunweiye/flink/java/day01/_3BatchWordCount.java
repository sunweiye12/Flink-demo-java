package com.sunweiye.flink.java.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class _3BatchWordCount {
    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String input = "file:///Users/bytedance/test.txt";   // (此处为文件夹目录,读取的是下面的 hello.txt)在根目录前加上 file://
        DataSource<String> text = env.readTextFile(input);

        FlatMapOperator<String, Tuple2<String, Integer>> wordAndOne = text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
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

        // 批处理中用的是的groupby不是keyby   -  可以为当前的算子提供一并行度,但是不能让并行度超过slot的数量
        AggregateOperator<Tuple2<String, Integer>> sumed = wordAndOne.groupBy(0).sum(1).setParallelism(2);

        // sink
        sumed.print();

        // 批处理可以省略
        // env.execute("12");
    }
}
