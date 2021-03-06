package com.daqsoft.api

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}

object KafkaSinkTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // Source操作
//    val streamFromFile = env.readTextFile("E:\\IdeaProjects\\flink-demo\\src\\main\\resources\\sensor.txt")

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.flink.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.flink.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    val inputStream = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))
    // Transform操作
    val dataStream = inputStream.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble).toString
    })

    // sink操作
    dataStream.addSink(new FlinkKafkaProducer011[String]("localhost:9092", "sink-test", new SimpleStringSchema()))

    env.execute("kafka sink test")
  }
}
