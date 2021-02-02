package com.junhua.apitest.tabletest

import java.time.Duration

import com.junhua.SensorReading
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.functions.TableAggregateFunction
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

object TableAggregateFunctionTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)


    //    val inputStream = env.socketTextStream("localhost", 7777)
    val inputStream = env.readTextFile("/Users/xiejunhua/github/learn-flink/input/input.csv")
      .map(
        data => {
          val dataArray = data.split(",")
          SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
        }
      ).assignTimestampsAndWatermarks(WatermarkStrategy
      .forBoundedOutOfOrderness[SensorReading](Duration.ofSeconds(3))
      .withTimestampAssigner(new SerializableTimestampAssigner[SensorReading] {
        override def extractTimestamp(element: SensorReading, recordTimestamp: Long): Long = element.timestamp * 1000
      }
      ))

    val tableEnv = StreamTableEnvironment.create(env)

        inputStream.print("test-input")
    val sensorTable = tableEnv.fromDataStream(inputStream, $("id"), $("temperature"), $("timestamp").rowtime().as("ts"))

    val top2Temp = new Top2Temp
    val resultTable = sensorTable.groupBy($"id")
      .flatAggregate(top2Temp($"temperature").as("temp", "rank"))
      .select($"id", $"temp", $"rank")

    resultTable.toRetractStream[Row].print()

    env.execute("test table agg function")


  }
}

// 定义一个类，用来表示表聚合函数到状态
class Top2TempAcc {
  var highestTemp:Double = Double.MinValue
  var secondHighestTemp:Double = Double.MinValue

}

//自定义表聚合函数，提取所有温度中最高到两个温度，输出（temperature， rank）
class Top2Temp extends TableAggregateFunction[(Double, Int), Top2TempAcc] {
  override def createAccumulator(): Top2TempAcc = new Top2TempAcc()

  def accumulate(acc:Top2TempAcc, temp:Double):Unit = {
    if (temp > acc.highestTemp) {
      acc.secondHighestTemp = acc.highestTemp
      acc.highestTemp = temp
    } else if (temp > acc.secondHighestTemp) {
      acc.secondHighestTemp = temp
    }
  }

  // 实现输出结果到方法
  def emitValue(acc:Top2TempAcc, out:Collector[(Double, Int)]):Unit = {
    out.collect((acc.highestTemp, 1))
    out.collect((acc.secondHighestTemp, 2))
  }
}
