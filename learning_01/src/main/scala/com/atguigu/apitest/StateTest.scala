package com.atguigu.apitest


import java.util

import org.apache.flink.api.common.functions.{RichFlatMapFunction, RichMapFunction}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor, ReducingStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector


/**
 * @author ZZY
 * @date 2021/1/8 15:10
 * @version 1.0
 */
object StateTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //    val inputStream = env.readTextFile("H:\\idea\\flink_Learning\\data\\score.csv")


    val inputStream: DataStream[String] = env.socketTextStream("localhost", 7777)

    // transform
    val dataStream = inputStream.map({
      data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
      }
    })

    val alterStream = dataStream.keyBy(_.id)
      //.flatMap(new TempAlter(10.0))
      //flatMapWithState方法：需要定义出入和输出的泛型
      //T:输入的类型  S：保存的状态
      //首次的Option[S]为：输入的状态，第二次的Option[S]为改变后的状态
      /*def flatMapWithState[R: TypeInformation, S: TypeInformation](
              fun: (T, Option[S]) => (TraversableOnce[R], Option[S])): DataStream[R] = {
       }*/
      .flatMapWithState[(String,Double,Double),Double]({
        //初始的时候没有温度
        case(data:SensorReading, None) => ( List.empty, Some(data.temperature) )

        case(data:SensorReading, lastTemp:Some[Double]) => {
            val diff = (data.temperature - lastTemp.get).abs
            if (diff >10.0){
              (List((data.id,lastTemp.get,data.temperature)),Some(data.temperature))
            }else{
              (List.empty,Some(data.temperature))
            }
          }
      })

    alterStream.print()

    env.execute("State Test")
  }

}

class TempAlter(threshold: Double) extends RichFlatMapFunction[SensorReading,(String,Double,Double)]{
  //定义状态保存上一次的温度
  lazy val lastTempState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("tempState",classOf[Double]))

  override def flatMap(value: SensorReading, out: Collector[(String, Double, Double)]): Unit = {

    val lastTemp = lastTempState.value()

    val diff = (value.temperature - lastTemp).abs
    if (diff > threshold ){
      out.collect((value.id,lastTemp,value.temperature))

      lastTempState.update(value.temperature)
    }
  }
}


//Keyed State测试：必须定义在richFunction中，因为需要运行上下文中
class MyRichMapper extends RichMapFunction[SensorReading,String]{

    var valueState: ValueState[Double] = _
  //list状态
  lazy val listState: ListState[Int] = getRuntimeContext
    .getListState(new ListStateDescriptor[Int]("list",classOf[Int]))
    //map状态
  lazy val mapState: MapState[String, Double] =getRuntimeContext
    .getMapState(new MapStateDescriptor[String,Double]("map",classOf[String],classOf[Double]))

    //聚合状态
//  lazy val reduceState = getRuntimeContext
//  .getReducingState(new ReducingStateDescriptor[SensorReading]("reduce",new MyReducer,classOf[SensorReading]))


  override def open(parameters: Configuration): Unit = {
    valueState = getRuntimeContext
      .getState(new ValueStateDescriptor[Double]("value",classOf[Double]))
  }

  override def map(value: SensorReading): String = {
    val myValue = valueState.value()
    valueState.update(value.timestamp)


    listState.add(1)
    val list = new util.ArrayList[Int]()
    list.add(1)
    list.add(2)
    listState.addAll(list)

    value.id
  }
}
