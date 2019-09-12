package com.flink.common.richf

import com.flink.common.bean.AdlogBean
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

class WordCountRichFunction  extends RichFlatMapFunction[(String, Int), (String, Int)] {
  var lastState: ValueState[Int] = _
  val ttlConfig = StateTtlConfig
    .newBuilder(Time.seconds(1000))
    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
    .build();

  /**
   * @author LMQ
   * @desc 每次一套
   * @param value
   * @param out
   */
  override def flatMap(value: (String, Int), out: Collector[(String, Int)]): Unit = {
    val ls = lastState.value()
    val nCount = ls + value._2
    lastState.update(nCount)
    out.collect((value._1, nCount))
  }

  override def open(parameters: Configuration): Unit = {
    val desc = new ValueStateDescriptor[Int](
      "count",
      classOf[Int],
      0)
    desc.enableTimeToLive(ttlConfig) // TTL
    //desc.setQueryable("StatisticalIndic")
    lastState = getRuntimeContext().getState(desc)
  }
}