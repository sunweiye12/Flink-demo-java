package com.sunweiye.flink.java.basic_api_concepts;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

/**
 * 通过 Tuple 来获取包装返回值进项处理
 */
public class _02自定义Key {
    public static void main(String[] args) throws Exception {

        // 1.构建执行环境
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2.读取数据
        String input = "file:///Users/bytedance/test.txt";   // 下面的 hello.txt *file://
        DataSource<String> text = env.readTextFile(input);

        // 3.数据处理  (开发核心)
        text.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void flatMap(String text, Collector<Tuple2<String, Integer>> collector) throws Exception {
                // 将读取的数据按招分隔符来进行切割
                String[] tokens = text.toLowerCase().split("\t");
                // 将每一个单词形成一个Tuple加上他们的次数1
                for (String token:tokens) {
                    if (token.length() > 0) {
                        collector.collect(new Tuple2<String,Integer>(token,1));
                    }
                }
            }
        }).groupBy(0)
                .sum(1)
                .print();

    }
}
