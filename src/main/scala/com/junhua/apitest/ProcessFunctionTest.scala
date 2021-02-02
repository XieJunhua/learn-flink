package com.junhua.apitest

import com.junhua.SensorReading
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object ProcessFunctionTest {

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
    val resultStream = dataStream
      .keyBy(_.id)
      .process(new TempIncreWarning(10000L))

    resultStream.print()

    env.execute("process function test")
  }
}

class TempIncreWarning(interval: Long) extends KeyedProcessFunction[String, SensorReading, String] {

  // 定义状态,保存温度值进行比较，保存时间戳用于删除
  lazy val lastTempState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("last-temp", classOf[Double]))
  lazy val timerTimestampState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("last-timestamp", classOf[Long]))

  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {
    val lastTemp = lastTempState.value()
    val timerTimestamp = timerTimestampState.value()

    lastTempState.update(value.temperature)


    // 如果温度上升，且没有定时器
    if (value.temperature > lastTemp && timerTimestamp == 0) {
      val ts = ctx.timerService().currentProcessingTime() + interval
      ctx.timerService().registerProcessingTimeTimer(ts)
      timerTimestampState.update(ts)
    } else if (value.temperature < lastTemp) {
      ctx.timerService().deleteProcessingTimeTimer(timerTimestamp)
      timerTimestampState.clear()
    }

  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    out.collect("传感器" + ctx.getCurrentKey + "的温度连续" + interval / 1000 + "秒上升")
    timerTimestampState.clear()
  }
}

class MyKeyedProcessFunction extends KeyedProcessFunction[String, SensorReading, String] {
  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {
    ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 6666) //注册定时器，process执行的时间

  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    // 定时器到了之后具体代码
  }
}
