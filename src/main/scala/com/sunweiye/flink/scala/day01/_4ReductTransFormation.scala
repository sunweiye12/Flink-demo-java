package com.sunweiye.flink.scala.day01

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala._

object _4ReductTransFormation {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val lines = env.socketTextStream("localhost", 9999)
    val words: DataStream[String] = lines.flatMap(_.split(" "))
    val keyed: KeyedStream[(String, Int), Tuple] = words.map((_, 1)).keyBy(0)

    val sumed: DataStream[(String, Int)] = keyed.reduce((m, n) => {
      val key = m._1
      val count1 = m._2
      val count2 = m._2
      (key, count1 + count2)
    })

    sumed.print()

    // scala 不用自己抛出异常
    env.execute("test")
  }

}
