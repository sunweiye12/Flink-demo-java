package com.sunweiye.flink.java.day02;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class _1SourceDemo {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 获取样例数据一般在测试的时候会用
        DataStreamSource<Integer> nums = env.fromElements(1, 2, 3, 4, 5, 6, 7);


    }
}
