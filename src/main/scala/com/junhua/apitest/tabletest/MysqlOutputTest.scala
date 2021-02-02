package com.junhua.apitest.tabletest

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}

object MysqlOutputTest {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

//    val inputStream = env.socketTextStream("localhost", 7777)

    val tableEnv = StreamTableEnvironment.create(env)



    // 需要先创建mysql表，flink不会帮你创建
    val sinkDDL:String =
      """
        |create table jdbcOutputTable(
        |    id varchar(20) not null,
        |    cnt bigint not null,
        |        primary key (id) not enforced
        |) with (
        |    'connector' = 'jdbc',
        |    'url' = 'jdbc:mysql://localhost:3306/test',
        |    'table-name' = 'sensor_count',
        |    'username' = 'root',
        |    'password' = '123456'
        |)
        |""".stripMargin

    val result: TableResult = tableEnv.executeSql(sinkDDL)


    result.print()


    tableEnv.connect(new FileSystem().path("/Users/xiejunhua/github/learn-flink/input/input.csv"))
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .field("temperature", DataTypes.DOUBLE())
      ).createTemporaryTable("inputTable")



    val inputTable: Table = tableEnv.from("inputTable").select($"id", $"timestamp")
    val aggTable = inputTable.groupBy($"id").select($"id", $"id".count().as("cnt"))
    val result1 = aggTable.executeInsert("jdbcOutputTable")
    result1.print()

//    env.execute("mysql table test")

  }
}
