package com.sunweiye.flink.java.day02;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class _1SourceDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 获取样例数据一般在测试的时候会用
        DataStreamSource<Integer> nums = env.fromElements(1, 2, 3, 4, 5, 6, 7);
        //DataStreamSource<Integer> nums1 = env.fromCollection(Arrays.asList(1, 2, 3, 4, 5, 6, 7));
        System.out.println("-----"+nums.getParallelism());      // 以上两个source都是单一并行度的

        SingleOutputStreamOperator<Integer> filtered = nums.filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer integer) throws Exception {
                return integer%2 == 0;
            }
        });

        System.out.println("-----"+filtered.getParallelism());
        filtered.print();

        env.execute("demo");
    }
}
