package com.sunweiye.flink.scala.wordcount

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

object StreamingWCScalaApp {

  def main(args: Array[String]): Unit = {

    // 1 获取上下文
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 2 获取数据源
    val source = env.socketTextStream("localhost",9999)

    // 3 对数据进行处理
    // TODO... 几行代码的含义(隐式转换,面试的重点)
    source.flatMap(_.toLowerCase.split(","))   // _ 下换线代表传进来内容
      .filter(_.nonEmpty)
      .map((_,1))
      .keyBy(0)   // 按照 map 的第一噶参数来归类
      .timeWindow(Time.seconds(5))
      .sum(1)   // 将第二个参数来相加
      .print()
      .setParallelism(1)

    env.execute("StreamingWCScalaApp")
  }

}
