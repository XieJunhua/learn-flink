package com.junhua.apitest.tabletest

import com.junhua.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
//import org.apache.flink.api.scala._

object Example {

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

    // create table env
    val tableEnv = StreamTableEnvironment.create(env)




    // 基于流创建一张表
    val dataTable: Table = tableEnv.fromDataStream(dataStream)

    tableEnv.createTemporaryView("dataTable", dataTable)

    val resultTable = dataTable.select($("id"), $("temperature"))

    val sql = "select id, temperature from dataTable"


    val resultTable1 = tableEnv.sqlQuery(sql)

    resultTable.toAppendStream[(String, Double)].print("result")
    resultTable1.toAppendStream[(String, Double)].print("sql")


    env.execute("table api test")
  }

}
