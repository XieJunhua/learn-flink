package com.junhua.apitest.tabletest

import java.time.Duration

import com.junhua.SensorReading
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}
import org.apache.flink.types.Row

object TableAndWindowTest {

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

    val resultTable = sensorTable
        .window(Tumble.over(10.seconds()).on($"ts").as("tw")) // 每10秒统计一次，滚动时间窗口
//        .window(Tumble over 10.seconds() on $"ts" as $"tw") //简单写法
      .groupBy( $"tw", $"id")
      .select($"id", $"id".count, $"temperature".avg, $"tw".end())

    tableEnv.createTemporaryView("sensor", sensorTable)


    val resultSqlTable = tableEnv.sqlQuery(
      """
        |select
        |id,
        |count(id),
        |avg(temperature),
        |tumble_end(ts, interval '10' second)
        |
        |from sensor
        |group by id,
        |tumble(ts, interval '10' second)
        |""".stripMargin)


    // 统计之前两行到当前行到平均数
    val overResultTable = sensorTable
      .window(Over.partitionBy("id").orderBy($"ts").preceding(2.rows).as("ow"))
        .select($"id", $"ts", $"id".count.over($"ow"), $"temperature".avg().over($"ow"))

    val overSqlTable = tableEnv.sqlQuery(
      """
        | select
        | id,
        | ts,
        | count(id) over ow,
        | avg(temperature) over ow
        |from sensor
        | window ow as (
        | partition by id
        | order by ts
        | rows between 2 preceding and current row
        | )
        |""".stripMargin)

//    resultTable.toAppendStream[Row].print("result")
//    resultSqlTable.toRetractStream[Row].print("sql")

    overResultTable.toAppendStream[Row].print("over")
    overSqlTable.toAppendStream[Row].print("overSql")





    env.execute("dd")



  }
}
