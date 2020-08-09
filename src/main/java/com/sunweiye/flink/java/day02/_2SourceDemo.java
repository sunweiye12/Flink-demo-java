package com.sunweiye.flink.java.day02;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.NumberSequenceIterator;


public class _2SourceDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 获取样例数据一般在测试的时候会用
        DataStreamSource<Long> longNum = env.fromParallelCollection(new NumberSequenceIterator(1, 10), Long.class);
        DataStreamSource<Long> longNum1 = env.fromParallelCollection(new NumberSequenceIterator(1, 10), TypeInformation.of(Long.TYPE));
        DataStreamSource<Long> longNum3 = env.generateSequence(1, 10);
        // 以上两个source都是多个并行度的(有多少可用的平行度,就是多少,但是可以设置并行度)
        System.out.println("-----"+longNum3.getParallelism());

        SingleOutputStreamOperator<Long> filtered = longNum3.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long integer) throws Exception {
                return integer%2 == 0;
            }
        }).setParallelism(3);

        System.out.println("-----"+filtered.getParallelism());
        filtered.print();

        env.execute("demo");
    }
}
