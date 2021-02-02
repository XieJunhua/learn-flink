package com.junhua.apitest

import com.junhua.SensorReading
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector


object SideOutputTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment


    env.enableCheckpointing(500, CheckpointingMode.EXACTLY_ONCE)

    env.getCheckpointConfig.setCheckpointTimeout(60000L)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)

//    env.setRestartStrategy(RestartStrategies.failureRateRestart())

    env.setParallelism(1)

    val inputStream = env.socketTextStream("localhost", 7777)

    val dataStream = inputStream
      .map(
        data => {
          val dataArray = data.split(",")
          SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
        }
      )
    val resultStream = dataStream
        .process(new SplitTempProcessor(30))

    resultStream.print("high")
    resultStream.getSideOutput(new OutputTag[(String, Long, Double)]("low")).print("low")

    env.execute("process function test")
  }

}

class SplitTempProcessor(threshold:Double) extends ProcessFunction[SensorReading, SensorReading]{
  override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
    if (value.temperature > threshold) {
      out.collect(value)
    } else {
      ctx.output(new OutputTag[(String, Long, Double)]("low"), (value.id, value.timestamp, value.temperature))
    }
  }
}
