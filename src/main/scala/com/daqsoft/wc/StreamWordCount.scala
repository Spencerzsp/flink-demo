package com.daqsoft.wc

import org.apache.flink.streaming.api.scala._

//  流处理word count程序
object StreamWordCount {

  def main(args: Array[String]): Unit = {

    // 创建流处理的执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //接收一个socket文本流
    val dataStream = env.socketTextStream("daqsoft-bdsp-01", 7777)

    // 对每条数据进行处理
    val wordCountStream = dataStream.flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    // 输出打印，设置并行度
    wordCountStream.print().setParallelism(2)

    // 启动executor
    env.execute("stream word count")


  }

}
