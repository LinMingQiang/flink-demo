package com.flink.learn.entry

import com.alibaba.fastjson.JSON
import com.flink.common.core.FlinkEvnBuilder
import com.flink.common.deserialize.{TopicMessageDeserialize, TopicOffsetMsgDeserialize}
import com.flink.common.kafka.KafkaManager
import com.flink.learn.param.PropertiesUtil
import com.flink.learn.sink.OperatorStateBufferingSink
import org.apache.flink.streaming.api.scala._
object FlinkOperatorStateTest {
  val checkpointPath = "file:///C:\\Users\\mqlin\\Desktop\\testdata\\flink\\checkpoint\\FlinkOperatorStateTest"
  def main(args: Array[String]): Unit = {
    PropertiesUtil.init("proPath");
    val env = FlinkEvnBuilder.buildFlinkEnv(PropertiesUtil.param, checkpointPath, 60000) // 1 min
    val kafkasource = KafkaManager.getKafkaSource(TOPIC, BROKER,  new TopicOffsetMsgDeserialize())
    kafkasource.setCommitOffsetsOnCheckpoints(true)
    kafkasource.setStartFromLatest() //不加这个默认是从上次消费
    env
      .addSource(kafkasource)
      .map(x => (JSON.parseObject(x.msg).getString("dist"), 1))
      .keyBy(0)
      .sum(1)
      .addSink(new OperatorStateBufferingSink(1))

    env.execute("FlinkOperatorStateTest")
  }
}
