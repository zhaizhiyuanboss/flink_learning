package com.atguigu.apitest

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
/**
 * @author ZZY
 * @date 2021/1/7 9:40
 * @version 1.0
 */
object JDBCTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val inputStream = env.readTextFile("H:\\idea\\flink_Learning\\data\\score.csv")

    // transform
    val dataStream = inputStream.map({
      data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
      }
    })


    dataStream.addSink(new myJdbcSink())

    env.execute("jdbc sink test")
  }
}

class myJdbcSink() extends RichSinkFunction[SensorReading]{
  // 定义sql连接、预编译器
  var conn: Connection = _
  var insertStmt: PreparedStatement = _
  var updateStmt: PreparedStatement = _

  override def open(parameters: Configuration): Unit = {
    conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "123456")
    insertStmt = conn.prepareStatement("INSERT INTO temperatures (sensor, temp) VALUES (?,?) ")
    updateStmt = conn.prepareStatement("UPDATE temperatures SET temp = ? WHERE sensor = ?")
  }


  override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
    updateStmt.setDouble(1,value.temperature)
    updateStmt.setString(2,value.id)
    updateStmt.execute()
    if(updateStmt.getUpdateCount == 0){
      insertStmt.setString(1,value.id)
      insertStmt.setDouble(2,value.temperature)
      insertStmt.execute()
    }
  }

  override def close(): Unit = {
    insertStmt.close()
    updateStmt.close()
    conn.close()
  }
}
