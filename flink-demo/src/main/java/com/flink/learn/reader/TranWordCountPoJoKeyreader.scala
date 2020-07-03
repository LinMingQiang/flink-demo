package com.flink.learn.reader

import com.flink.learn.bean.TranWordCountPoJo
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.state.api.functions.KeyedStateReaderFunction
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.util.Collector

class TranWordCountPoJoKeyreader(stateName : String)
  extends KeyedStateReaderFunction[String, TranWordCountPoJo] {


  var lastState: ValueState[TranWordCountPoJo] = _
  override def open(parameters: Configuration): Unit = {
    val desc = new ValueStateDescriptor[TranWordCountPoJo](
      stateName,
      createTypeInformation[TranWordCountPoJo])
    lastState = getRuntimeContext().getState(desc)
  }

  override def readKey(key: String,
                       context: KeyedStateReaderFunction.Context,
                       collector: Collector[TranWordCountPoJo]): Unit = {
    val w = new TranWordCountPoJo()
    val v = lastState.value()
    w.word = v.word
    w.count = v.count;
    w.timestamp = v.timestamp
    collector.collect(w)
  }
}