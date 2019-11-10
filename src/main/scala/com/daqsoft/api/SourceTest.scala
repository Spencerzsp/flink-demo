package com.daqsoft.api

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import scala.util.Random

case class SensorReading(id: String, time: Long, temprature: Double)
object SourceTest {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置全局并行度
    env.setParallelism(1)

    // 1.从自定义的集合中获取数据
    val stream1 = env.fromCollection(List(
      SensorReading("sensor_1", 147258369, 28.123456),
      SensorReading("sensor_2", 147258370, 8.123456),
      SensorReading("sensor_6", 147258388, 18.123456),
      SensorReading("sensor_5", 147258390, 38.123456)
    ))
//    stream1.print("stream1").setParallelism(1)

    // 2.从文件中获取数据
    val stream2 = env.readTextFile("E:\\IdeaProjects\\flink-demo\\src\\main\\resources\\sensor.txt")
//    stream2.print("stream2")

    // 3.从kafka读取数据
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.flink.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.flink.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    val stream3 = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))
//    stream3.print("stream3")

    // 4.自定义Source
    val stream4 = env.addSource(new SensorSource())
    stream4.print("stream4").setParallelism(1)


    env.execute("source test")
  }

  class SensorSource() extends SourceFunction[SensorReading]{

    var running: Boolean = true

    override def cancel(): Unit = {
      running = false
    }

    override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {

      // 初始化一个随机数发生器
      val random = new Random()

      // 初始化一组传感器温度数据
      var curTemp = 1.to(10).map(
        i => ("sensor_" + i, 60 + random.nextGaussian()
      ))

      // 用无限循环产生数据流
      while (running){
        // 在前一次温度的基础上更新温度值
        curTemp = curTemp.map(
          t => (t._1, t._2 + random.nextGaussian())
        )
        // 获取当前时间
        val curTime = System.currentTimeMillis()
        curTemp.foreach(
          // 发送数据
          t => sourceContext.collect(SensorReading(t._1, curTime, t._2))
        )
        Thread.sleep(500)
      }
    }
  }

}
