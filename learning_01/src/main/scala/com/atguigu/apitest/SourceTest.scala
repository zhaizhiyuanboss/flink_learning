package com.atguigu.apitest



import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
 * @author ZZY
 * @date 2021/1/5 14:35
 * @version 1.0
 */
//定义样例类
case class SensorReading(id:String,timestamp: Long,temperature: Double)

object SourceTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.createLocalEnvironment()

    //1.从集合中拿数据
    val result: DataStream[String] = env.fromElements(
      SensorReading("123", 124, 23.1).toString
    )


    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "test")
    val result1: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("topic", new SimpleStringSchema(), properties))

    val dataStream: DataStream[String] = result1.map(
      data => {
        val arr: Array[String] = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble).toString
      }
    )
    val dataStream1: DataStream[String] = result1.map(
      data => {
        val arr: Array[String] = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble).toString
      }
    )


    dataStream.addSink(new FlinkKafkaProducer[String]("localhost:9092","test",new SimpleStringSchema()))


    //定义FlinkJedisConfigBase
    val conf: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder()
      .setHost("localhost")
      .setPort(6379)
      .build()


    dataStream1.addSink( new RedisSink[SensorReading]())
    env.execute()

  }


}

class MyRedisMapper extends RedisMapper[SensorReading]{
  //定义保存数据写入redis的命令，HSET 表名  key，value
  override def getCommandDescription: RedisCommandDescription =
    new RedisCommandDescription(RedisCommand.HSET,"sensor_temp")

  override def getKeyFromData(data: SensorReading): String = data.temperature.toString

  override def getValueFromData(data: SensorReading): String = data.id
}

