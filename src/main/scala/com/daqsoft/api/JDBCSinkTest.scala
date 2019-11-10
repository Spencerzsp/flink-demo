package com.daqsoft.api

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

object JDBCSinkTest {

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

    // Sink

    dataStream.addSink(new MyJdbcSink())
  }
}

class MyJdbcSink() extends RichSinkFunction[SensorReading]{

  // 定义sql连接、预编译器
  var connection: Connection = _
  var insertStmt: PreparedStatement = _
  var updateStmt: PreparedStatement = _

  override def open(parameters: Configuration) = {
    super.open(parameters)
    connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "123456")
    insertStmt = connection.prepareStatement("insert into test(sensor, temp)values(?, ?)")
    updateStmt = connection.prepareStatement("update test set temp = ? where sensor = ?")
  }

  // 调用连接，执行sql
  override def invoke(value: SensorReading, context: SinkFunction.Context[_]) = {
    updateStmt.setDouble(1, value.temprature)
    updateStmt.setString(2, value.id)
    updateStmt.execute()

    // 如果update没有查到数据，那么执行插入数据
    if(updateStmt.getUpdateCount == 0) {
      insertStmt.setString(1, value.id)
      insertStmt.setDouble(2, value.temprature)
      insertStmt.execute()
    }
  }

  // 关闭连接
  override def close() = {
    insertStmt.close()
    updateStmt.close()
    connection.close()
  }
}
