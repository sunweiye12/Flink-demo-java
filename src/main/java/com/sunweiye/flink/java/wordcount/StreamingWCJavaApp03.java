package com.sunweiye.flink.java.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 指定 key 为字符串->将处理后的数据封装到一个对象中的然后再根据字段处理
 */
public class StreamingWCJavaApp03 {

    public static void main(String[] args) throws Exception {

        String hostname;
        int port;
        // 通过 Parameter 来获取参数,其中参数在运行时设置(--key value 的形式)
        ParameterTool tool = ParameterTool.fromArgs(args);
        // 因为在获取参加的过程中可能会存在错误,因此这种环节要在 try catch 中执行
        try{
            port = tool.getInt("port");
            hostname = tool.get("hostname");
        } catch (Exception e) {
            System.err.println(e);
            hostname = "localhost";
            port = 9999;
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("localhost",port);

        // 输入String数据,返回一个定义好的 WC 对象
        source.flatMap(new FlatMapFunction<String, WC>() {
            @Override
            public void flatMap(String text, Collector<WC> collector) throws Exception {
                // 将读取的数据逗号进行切割
                String[] tokens = text.toLowerCase().split(",");
                for (String token:tokens) {
                    if (token.length() > 0) {
                        collector.collect(new WC(token,1)); // 将收集的数据封装成一个 WC 对象并指定 count 为 1
                    }
                }
            }
        }).keyBy("word")                     // 根据 WC 对象的 word 字段来进行 keyBy
                .timeWindow(Time.seconds(5))
                .sum("count")              // 对 WC 对象的 count 字段来进行求和
                .setParallelism(1)
                .print();

        // 流式开发不许要执行环境
        env.execute("StreamingWCJavaApp");
    }

    public static class WC{
        private String word;
        private int count;

        public WC() {
        }

        public WC(String word, int count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return "WC{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }
    }
}
