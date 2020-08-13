package com.sunweiye.flink.scala.day01

import org.apache.flink.streaming.api.scala._

object _3StreamWordCount {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val lines: DataStream[String] = env.socketTextStream("localhost", 9999)

    val words: DataStream[String] = lines.flatMap(_.split(" "))

    val wordAndOne:DataStream[(String,Int)] = words.map((_,1))

    val sumed: DataStream[(String, Int)] = wordAndOne.keyBy(0).sum(1)

    sumed.print()

    // scala 不用自己抛出异常
    env.execute("test")
  }
}
