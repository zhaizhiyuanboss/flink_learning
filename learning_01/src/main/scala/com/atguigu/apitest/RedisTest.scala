package com.atguigu.apitest

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
 * @author ZZY
 * @date 2021/1/6 16:27
 * @version 1.0
 */

// 定义传感器数据样例类
case class SensorReading( id: String, timestamp: Long, temperature: Double )

object RedisTest {
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

    val conf = new FlinkJedisPoolConfig.Builder()
      .setHost("localhost")
      .setPort(6379)
      .build()

    dataStream.addSink(new RedisSink[SensorReading](conf,new MyRedisMapper()))

    env.execute("redis sink test")
  }
}

class MyRedisMapper() extends RedisMapper[SensorReading]{

  // 定义保存数据到redis的命令
  override def getCommandDescription: RedisCommandDescription = {
    // 把传感器id和温度值保存成哈希表 HSET key field value
    new RedisCommandDescription( RedisCommand.HSET, "sensor_temperature" )
  }

  // 定义保存到redis的value
  override def getValueFromData(t: SensorReading): String = t.temperature.toString

  // 定义保存到redis的key
  override def getKeyFromData(t: SensorReading): String = t.id
}
