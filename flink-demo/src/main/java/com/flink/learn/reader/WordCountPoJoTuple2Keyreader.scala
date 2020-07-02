package com.flink.learn.reader

import com.flink.learn.bean.{TranWordCountPoJo, WordCountPoJo}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.state.api.functions.KeyedStateReaderFunction
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.util.Collector

class WordCountPoJoTuple2Keyreader(stateName: String)
    extends KeyedStateReaderFunction[
      org.apache.flink.api.java.tuple.Tuple2[String, String],
      TranWordCountPoJo] {
  var lastState: ValueState[WordCountPoJo] = _
  override def open(parameters: Configuration): Unit = {
    val desc = new ValueStateDescriptor[WordCountPoJo](
      stateName,
      createTypeInformation[WordCountPoJo])
    lastState = getRuntimeContext().getState(desc)
  }

  override def readKey(
      key: org.apache.flink.api.java.tuple.Tuple2[String, String],
      context: KeyedStateReaderFunction.Context,
      collector: Collector[TranWordCountPoJo]): Unit = {
    val w = new TranWordCountPoJo()
    val v = lastState.value()
    w.word = v.word
    w.count = v.count
    w.timestamp = v.timestamp
    collector.collect(w)
  }
}
