package com.sunweiye.flink.scala.day01

import org.apache.flink.api.scala._

object _1BatchWordCount {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    // 2 获取数据源
    val input = "file:///Users/bytedance/test.txt"
    val text: DataSet[String] = env.readTextFile(input)

    val words: DataSet[String] = text.flatMap(_.split("\t"))
    val wordAndOne: DataSet[(String, Int)] = words.map((_, 1))
    // 算子进行计算的时候开启2个并行度
    val sumed = wordAndOne.groupBy(0).sum(1).setParallelism(2)

    // sink
    sumed.print()
    env.execute("test")
  }
}
