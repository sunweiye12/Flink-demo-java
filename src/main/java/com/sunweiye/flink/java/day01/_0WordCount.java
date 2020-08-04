package com.sunweiye.flink.java.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 使用 java 来开发一个流式的开发应用数据程序
 * 程序需启动后会持续监听本地9999端口,通过 nc -lk 9999 命令向里面持续写数据,程序会将每次写的数据切分成元素来进行统计
 * 切分后的元素会落到某个slot里面进行统计,相通的元素一定会落到同一个slot中,slot数目和我们设置的并行度相通,本地测试默认的是本机的逻辑核数
 * (solt底层是从0开始计数的,但是在输出的时候为了方便观察对其进行了+1处理)
 */
public class _0WordCount {

    public static void main(String[] args) throws Exception {

        // 1.获取上下文
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2.通过 socket 获取实时的数据流(读取本地 9999 端口 socket 的数据)
        DataStreamSource<String> lines = env.socketTextStream("localhost",9999);

        // 3.核心操作1 (将输入的数据通过flatmap进行切除,将一个元素切分多个过着0个)
        // flatmap函数第一个参数是指输入的数据,第二个参数是输出数据的类型 ---> 将输入的String转换成0-n个String来输出
        SingleOutputStreamOperator<String> words = lines.flatMap(new FlatMapFunction<String,String>() {
            @Override
            public void flatMap(String text, Collector<String> collector) throws Exception {
                // 将读取数据切分
                String[] tokens = text.toLowerCase().split(" ");
                for (String token:tokens) {
                    if (token.length() > 0) {
                        // 输出
                        collector.collect(token);
                    }
                }
            }
        });

        // 3.核心操作2 将单词和一组合
        SingleOutputStreamOperator<Tuple2<String,Integer>> wordAndOne = words.map(new MapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public Tuple2 map(String word) throws Exception {
                return Tuple2.of(word,1);
            }
        });

        // 3.核心操作3 以第一个字段进行分组,将第二个字段进行相加
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumned = wordAndOne.keyBy(0).sum(1);

        // 4 操作结束,输出结果(Sink调用)
        sumned.print();

        // 5 启动程序
        env.execute("WordCunt");
    }
}
