package com.sunweiye.flink.java.basic_api_concepts;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 通过自定义KeySelect来获取 Key
 */
public class _04自定义Key {
    public static void main(String[] args) throws Exception {

        // 1.获取上下文
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2.通过 socket 获取实时的数据流(读取本地 9999 端口 socket 的数据)
        DataStreamSource<String> source = env.socketTextStream("localhost",9999);

        // 输入String数据,返回一个定义好的 WC 对象
        source.flatMap(new FlatMapFunction<String, WC>() {
            @Override
            public void flatMap(String text, Collector<WC> collector) throws Exception {
                // 将读取的数据逗号进行切割
                String[] tokens = text.toLowerCase().split(",");
                for (String token:tokens) {
                    if (token.length() > 0) {
                        collector.collect(new WC(token,1));
                    }
                }
            }
        }).keyBy(new KeySelector<WC, Object>() {            //  ***在此处
            @Override
            public Object getKey(WC wc) throws Exception {
                return wc.word;
            }
        }).timeWindow(Time.seconds(5))
                .sum("count")
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
