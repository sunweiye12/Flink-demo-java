package com.sunweiye.flink.scala.wordcount

import org.apache.flink.api.scala._

object BatchWCScalaApp {

  def main(args: Array[String]): Unit = {

    // 1 获取上下文
    val env = ExecutionEnvironment.getExecutionEnvironment

    // 2 获取数据源
    val input = "file:///Users/sunweiye/tem/flink/input"
    val text = env.readTextFile(input)
    text.print()

    // 3 对数据进行处理
    // TODO... 几行代码的含义(隐式转换,面试的重点)
    text.flatMap(_.toLowerCase.split("\t"))   // _ 下换线代表传进来内容
      .filter(_.nonEmpty)
      .map((_,1))
      .groupBy(0)   // 按照 map 的第一噶参数来归类
      .sum(1).print()   // 将第二个参数来相加
  }

}
