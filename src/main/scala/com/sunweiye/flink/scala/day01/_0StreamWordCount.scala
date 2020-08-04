package com.sunweiye.flink.scala.day01

import org.apache.flink.streaming.api.scala._

object _0StreamWordCount {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //Source
    val lines: DataStream[String] = env.socketTextStream("localhost", 9999)

    // Transformation开始
    val words: DataStream[String] = lines.flatMap(_.split(" "))

    val wordAndOne: DataStream[(String, Int)] = words.map((_, 1))

    val sumed: DataStream[(String, Int)] = wordAndOne.keyBy(0).sum(1)

    // sink
    sumed.print()
    env.execute("scala wordcount")
  }
}
