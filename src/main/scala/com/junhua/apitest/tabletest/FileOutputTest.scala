package com.junhua.apitest.tabletest

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}


object FileOutputTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val inputStream = env.socketTextStream("localhost", 7777)

    val tableEnv = StreamTableEnvironment.create(env)

    tableEnv.connect(new FileSystem().path("/Users/xiejunhua/github/learn-flink/input/input.csv"))
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .field("temperature", DataTypes.DOUBLE())
      ).createTemporaryTable("inputTable")

    val inputTable: Table = tableEnv.from("inputTable").select($"id", $"timestamp")
    val aggTable = inputTable.groupBy($"id").select($"id", $"id".count().as("count"))


//    inputTable.toAppendStream[(String, Long)].print("result")
//    aggTable.toRetractStream[(String, Long)].print("agg") // 这里不能直接输出appendStream

    /**
     * agg> (false,(sensor_1,5))
     * agg> (true,(sensor_1,6))
     * agg> (false,(sensor_1,6)) // false表示老的已经失效了
     * agg> (true,(sensor_1,7)) // true表示当前新的值
     * agg> (false,(sensor_1,7))
     * agg> (true,(sensor_1,8))
     */

    // 输出到文件
    tableEnv.connect(new FileSystem().path("/Users/xiejunhua/github/learn-flink/input/output.csv"))
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
      ).createTemporaryTable("outputTable")

    inputTable.executeInsert("outputTable")

    env.execute("output file")

  }
}
