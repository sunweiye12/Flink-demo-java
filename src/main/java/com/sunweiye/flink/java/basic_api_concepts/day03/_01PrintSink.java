package com.sunweiye.flink.java.basic_api_concepts.day03;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class _01PrintSink {

    public static void main(String[] args) throws Exception {

        // 1.获取上下文
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2.通过 socket 获取实时的数据流(读取本地 9999 端口 socket 的数据)
        DataStreamSource<String> lines = env.socketTextStream("localhost",9999);

        lines.print("swy");

        env.execute("WordCunt");
    }
}
