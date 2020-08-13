package com.sunweiye.flink.java.day02;

import com.sunweiye.flink.java.WCBean;
import org.apache.commons.math3.fitting.leastsquares.EvaluationRmsChecker;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class _9KeyByFormationConsumer {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<WCBean> wcBean = lines.flatMap(new FlatMapFunction<String, WCBean>() {
            @Override
            public void flatMap(String text, Collector<WCBean> collector) throws Exception {
                // 将读取数据切分
                String[] words = text.toLowerCase().split(" ");
                for (String word : words) {
                    collector.collect(new WCBean(word, 2));
                }
            }
        });

        // 按照指定的字段进行分类,按照另一个字段进行相加统计
        SingleOutputStreamOperator<WCBean> named = wcBean.keyBy("name").sum("count");

        named.print();

        env.execute("TransFormation");
    }
}
