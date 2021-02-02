package com.junhua.apitest

import com.junhua.SensorReading
import org.apache.flink.api.common.functions.{RichFlatMapFunction, RichMapFunction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object StateTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val inputStream = env.socketTextStream("localhost", 7777)

    val dataStream = inputStream
      .map(
        data => {
          val dataArray = data.split(",")
          SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
        }
      )
    // 需求：对于温度跳变超过10度报警

    val alertStream = dataStream
      .keyBy(_.id)
//      .flatMap(new TempChangeAlert(10.0)) //自定义flatmap的写法
        .flatMapWithState[(String, Double, Double), Double]({
          case (data:SensorReading, None) => (List.empty, Some(data.temperature))
          case (data:SensorReading, lastTemp:Some[Double]) =>
            val diff = (data.temperature - lastTemp.get).abs
            if (diff >= 10.0) {
              (List((data.id, lastTemp.get, data.temperature)), Some(data.temperature))
            }else{
              (List.empty, Some(data.temperature))
            }
        })

    alertStream.print()

    env.execute("state test")

  }
}


class TempChangeAlert(threshold:Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)] {

  // 保存上次温度值
  lazy val lastTempState:ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("temp", classOf[Double]))

  override def flatMap(value: SensorReading, out: Collector[(String, Double, Double)]): Unit = {
    val lastTemp = lastTempState.value()
    val diff = (value.temperature - lastTemp).abs
    if (diff >= threshold) {
      out.collect((value.id,lastTemp, value.temperature))
    }

    lastTempState.update(value.temperature)

  }
}

class MyRichMapper extends RichMapFunction[SensorReading, String] {

  var valueState:ValueState[Double] = _


  override def open(parameters: _root_.org.apache.flink.configuration.Configuration): Unit = {
    valueState  = getRuntimeContext.getState(new ValueStateDescriptor[Double]("valueState ", classOf[Double]))
//    getRuntimeContext.getMapState()

  }

  override def map(value: SensorReading): String = {
    val myV = valueState.value()
    valueState.update(value.temperature)
    value.id
  }
}
