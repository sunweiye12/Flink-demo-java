package com.sunweiye.flink.java.day02;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class _5MapTransFormation {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> input = env.fromElements(1, 2, 3, 4, 5);

        //map做映射,输入为int输出也是int
        SingleOutputStreamOperator<Integer> out = input.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer i) throws Exception {
                return i * 2;
            }
        });

        // RichMapFunction函数一个功能更加丰富的MapFunction
        SingleOutputStreamOperator<Object> out1 = input.map(new RichMapFunction<Integer, Object>() {
            @Override
            public Object map(Integer integer) throws Exception {
                return null;
            }

            // 在构造方法后,map之前执行,用于打开驱动
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }

            //在subTask关闭的时候执行,用于释放资源
            @Override
            public void close() throws Exception {
                super.close();
            }
        });

        // sink
        out.print();

        env.execute("_5MapTransFormation");
    }
}
