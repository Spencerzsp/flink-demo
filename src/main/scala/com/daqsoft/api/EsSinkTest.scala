package com.daqsoft.api

import java.util

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

object EsSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // Source操作
    val inputStream = env.readTextFile("E:\\IdeaProjects\\flink-demo\\src\\main\\resources\\sensor.txt")

    // Transform
    val dataStream = inputStream.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })


    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("localhost", 9200))

    // 创建一个esSink的builder
    new ElasticsearchSink.Builder[SensorReading](
      httpHosts,
      new ElasticsearchSinkFunction[SensorReading] {
        override def process(t: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
          println("save data:" + t)

          // 包装成Map或者JsonObject
          val json = new util.HashMap[String, String]()
          json.put("sensor_id", t.id)
          json.put("temperature", t.temprature.toString)
          json.put("time", t.time.toString)

          // 创建index request，准备发送数据
          val indexRequest = Requests.indexRequest()
            .index("sensor")
            .`type`("readingdata")
            .source(json)

          // 利用index发送请求，写入数据
          requestIndexer.add(indexRequest)
          println("data saved")
        }

      }
    )
    // Sink
    dataStream.addSink()

    env.execute("es sink")
  }
}
