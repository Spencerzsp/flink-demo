package com.daqsoft.wc

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

object WordCount {

  def main(args: Array[String]): Unit = {

    // 创建一个执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    val inputPath = "E:\\IdeaProjects\\flink-demo\\src\\main\\resources\\hello.txt"
    val inputDataSet = env.readTextFile(inputPath)

    // 切分数据得到word，然后按照word做分组聚合
    val wordCount = inputDataSet.flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0)
      .sum(1)

    wordCount.print()

  }

}
