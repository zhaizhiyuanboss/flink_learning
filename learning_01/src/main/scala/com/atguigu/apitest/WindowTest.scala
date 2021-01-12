package com.atguigu.apitest

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @author ZZY
 * @date 2021/1/7 14:50
 * @version 1.0
 */
object WindowTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(500)

    val inputStream = env.readTextFile("H:\\idea\\flink_Learning\\data\\score.csv")


    // transform
    val dataStream = inputStream.map({
      data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
      }
    })//.assignAscendingTimestamps(_.timestamp * 1000L)
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds( 5)) {
        override def extractTimestamp(element: SensorReading): Long = {
          element.timestamp * 1000L
        }
      })



    dataStream
      .map(data => (data.id,data.temperature,data.timestamp))
      .keyBy(_._1)  //按照二元组的第一个元素进行分组
      //.window(TumblingEventTimeWindows.of(Time.seconds(5)))//滚动窗口
//      .window(SlidingEventTimeWindows.of(Time.seconds(15),Time.seconds(5)))//滑动窗口
      //.window(EventTimeSessionWindows.withGap(Time.seconds(5)))//会话窗口
      //.countWindow(10)
      .timeWindow(Time.seconds(15))
      .allowedLateness(Time.seconds(15))
      .sideOutputLateData(new OutputTag[(String, Double, Long)]("late"))
      .reduce((current,resData)=>(current._1,current._2.min(resData._2),resData._3))




    env.execute()
  }
}
