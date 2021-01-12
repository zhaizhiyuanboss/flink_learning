package com.atguigu.apitest

import java.util

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests
/**
 * @author ZZY
 * @date 2021/1/6 16:52
 * @version 1.0
 */

// 定义传感器数据样例类
case class SensorReading( id: String, timestamp: Long, temperature: Double )


object EsSinkTest {
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

    //定义httpHost
    val httpHost = new util.ArrayList[HttpHost]()
    httpHost.add(new HttpHost("localhost",9200))


    val myEsSinkFun = new ElasticsearchSinkFunction[SensorReading] {
      override def process(element: SensorReading, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
        //创建source
        val dataSource = new util.HashMap[String, String]()

        dataSource.put("id",element.id)
        dataSource.put("temp",element.temperature.toString)
        dataSource.put("time",element.timestamp.toString)

        //创建index request   用于发送http请求
        val indexRequest = Requests.indexRequest()
          .index("sensor")
          .`type`("readingdata")
          .source(dataSource)

        //向indexer发送请求
        indexer.add(indexRequest)
      }
    }

    dataStream.addSink(new ElasticsearchSink
    .Builder[SensorReading](httpHost,myEsSinkFun)
    .build())


  }
}
