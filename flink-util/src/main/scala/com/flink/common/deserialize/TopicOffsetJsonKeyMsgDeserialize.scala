package com.flink.common.deserialize

import com.alibaba.fastjson.JSON
import com.flink.common.kafka.KafkaManager._
import org.apache.commons.lang3.time.DateFormatUtils
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema
import org.apache.kafka.clients.consumer.ConsumerRecord

class TopicOffsetJsonKeyMsgDeserialize
  extends KafkaDeserializationSchema[KafkaTopicOffsetTimeUidMsg] {
  // {"ts":1,"msg":"1"}
  val t = 1600000000000L

  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]) = {
    val msg = new String(record.value())
    val msgJson = JSON.parseObject(msg)
    if (msgJson.containsKey("ts")) {
      KafkaTopicOffsetTimeUidMsg(
        new String(record.topic()),
        t + msgJson.getLong("ts") * 1000,
        msgJson.getString("uid"),
        msgJson.getString("msg")
      )
    } else {
      null
    }
  }

  override def isEndOfStream(nextElement: KafkaTopicOffsetTimeUidMsg) = {
    false
  }

  override def getProducedType() = {
    createTypeInformation[KafkaTopicOffsetTimeUidMsg]
      .asInstanceOf[CaseClassTypeInfo[KafkaTopicOffsetTimeUidMsg]]
  }
}
