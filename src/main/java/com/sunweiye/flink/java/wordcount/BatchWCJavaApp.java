package com.sunweiye.flink.java.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 使用 java 来开发第一个批处理应用程序
 */
public class BatchWCJavaApp {

    public static void main(String[] args) throws Exception {
        // 1.构建执行环境
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2.读取数据
        String input = "file:///Users/bytedance/test.txt";   // (此处为文件夹目录,读取的是下面的 hello.txt)在根目录前加上 file://
        DataSource<String> text = env.readTextFile(input);       // 根据输入开获得一个输入集
            //text.print();           // 通过这个方法来获取数据的数据

        // 3.数据处理  (开发核心)
            // 1.读取数据
            // 2.将每一行的数据按照分割符来分开
            // 3.将每个单词附上次数为 1
            // 4.合并操作,将单词的次数加起来
        // FlatMapFunction中两个参数其中一个是输入的String,另一个是处理后返回的类型(单词,次数)
        text.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void flatMap(String text, Collector<Tuple2<String, Integer>> collector) throws Exception {
                // 将读取的数据按照分隔符来进行切割
                String[] tokens = text.toLowerCase().split("\t");
                // 将每一个单词形成一个Tuple加上他们的次数 1
                for (String token:tokens) {
                    if (token.length() > 0) {
                        collector.collect(new Tuple2<String,Integer>(token,1));
                    }
                }
            }
        }).groupBy(0)       // 以第一个字段作为分组
                .sum(1)         // 对第二个字段做相加处理
                .print();

        // 4.执行程序
        // env.execute("Flink Batch Java API Skeleton");
    }
}
