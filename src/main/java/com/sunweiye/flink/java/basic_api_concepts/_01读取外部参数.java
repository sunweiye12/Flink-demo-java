package com.sunweiye.flink.java.basic_api_concepts;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

/**
 * 通过ParameterTool来获取 main 函数传进来的参数
 */
public class _01读取外部参数 {
    public static void main(String[] args) throws Exception {

        // 1.构建执行环境
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2.读取数据
            //String input = "file:///Users/bytedance/test.txt";   // (此处为文件夹目录,读取的是下面的 hello.txt)在根目录前加上 file://
        String input;
        // 通过 Parameter 来获取参数,其中参数在运行时设置(--key value 的形式)
        ParameterTool tool = ParameterTool.fromArgs(args);
        // 因为在获取参加的过程中可能会存在错误,因此这种环节要在 try catch 中执行
        try{
            input = tool.get("input");
        } catch (Exception e) {
            System.err.println("获取 input 参数失败已经自动赋值默认参数");
            input = "file:///Users/bytedance/test.txt";
        }

        DataSource<String> text = env.readTextFile(input);       // 根据输入开获得一个输入集

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
        }).groupBy(0).sum(1).print();

        // 4.执行程序
        // env.execute("Flink Batch Java API Skeleton");
    }
}
