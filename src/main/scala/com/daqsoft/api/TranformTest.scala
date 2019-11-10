package com.daqsoft.api

import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.streaming.api.scala._

object TranformTest {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val streamFromFile = env.readTextFile("E:\\IdeaProjects\\flink-demo\\src\\main\\resources\\sensor.txt")

    // 1.基本转换算子和简单聚合算子
    val dataStream = streamFromFile.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })
//      .keyBy("id")
//      .sum("temperature")
//      .keyBy(_.id)
//      .keyBy(0)
//      .sum(2)
//      .reduce((x, y) => SensorReading(x.id ,x.time+1, x.temprature+10))

    // 2.多流转换算子
    // 分流split
    val splitStream = dataStream.split(x => {
      if (x.temprature > 30) Seq("high") else Seq("low")
    })
    val high = splitStream.select("high")
    val low = splitStream.select("low")
    val all = splitStream.select("high", "low")

//    high.print("high")
//    low.print("low")
//    all.print("all")

    // 合并两条流
    val warning = high.map(data => (data.id, data.temprature))
    val connectedStream = warning.connect(low)

    val coMapDataStream = connectedStream.map(
      warningData => (warningData._1, warningData._2, "warning"),
      lowData => (lowData.id, "healthy")
    )
//    coMapDataStream.print()

    // 自定义函数类
    val filterStream = dataStream.filter(new MyFilter())
    filterStream.print("filterStream")

    env.execute("transform test")
  }

  class  MyFilter() extends FilterFunction[SensorReading]{
    override def filter(t: SensorReading): Boolean = {
      t.id.endsWith("0")
//      t.temprature > 20
//      t.id.contains("sensor_100")
    }
  }
}
