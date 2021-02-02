package com.junhua.apitest.tabletest

import java.time.Duration

import com.junhua.SensorReading
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.functions.{ScalarFunction, TableFunction}
import org.apache.flink.types.Row

object UdfTast {

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

    //    inputStream.print()
    val sensorTable = tableEnv.fromDataStream(inputStream, $("id"), $("temperature"), $("timestamp").rowtime().as("ts"))


    val hashCode = new HashCode(12)
    // table api使用
    val resultTable = sensorTable.select($"id", $"ts", hashCode($"id"))
    // sql使用
    tableEnv.createTemporarySystemFunction("hashcode", hashCode)
    tableEnv.createTemporaryView("sensor", sensorTable)
    val resultSqlTable = tableEnv.sqlQuery("select id, ts, hashcode(id) from sensor")

//    resultTable.toAppendStream[Row].print("result")
//    resultSqlTable.toRetractStream[Row].print("sql")


    val mySplit = new Split("_")
    val splitResultTable = sensorTable
      .joinLateral(mySplit($"id")
        .as("word", "length"))
        .select($"id", $"ts", $"word", $"length")

    tableEnv.createTemporarySystemFunction("mySplit", mySplit)
    val splitSqlTable = tableEnv.sqlQuery(
      """
        |select id, ts, word, length
        |from sensor, lateral table(mySplit(id)) as splitid(word, length)
        |""".stripMargin)


    splitResultTable.toAppendStream[Row].print("result")
    splitSqlTable.toRetractStream[Row].print("sql")


    env.execute("udf test")

  }
}

class HashCode(factor: Int) extends ScalarFunction {
  def eval(s: String): Int = {
    s.hashCode * factor - 10000
  }
}

class Split(separator:String) extends TableFunction[(String, Int)] {
  def eval(str:String):Unit = {
    str.split(separator).foreach(word => collect((word, word.length)))
  }

}
