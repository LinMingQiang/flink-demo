package com.flink.common.deserialize

import com.alibaba.fastjson.JSON
import com.flink.common.kafka.KafkaManager._
import org.apache.commons.lang3.time.DateFormatUtils
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema
import org.apache.kafka.clients.consumer.ConsumerRecord

class TopicOffsetJsonReqImpClickDeserialize
  extends KafkaDeserializationSchema[KafkaTopicReqImpClickMsg] {
  val t = 1600000000000L
  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]) = {
    val msg = new String(record.value())
    val msgJson = JSON.parseObject(msg)
    if(msgJson.containsKey("ts")) {
        KafkaTopicReqImpClickMsg(msgJson.getString("log"),
          t + msgJson.getLong("ts")*1000,
          msgJson.getString("reqid"),
          msgJson.getString("msg")
        )
      } else {
        null
      }
  }

  override def isEndOfStream(nextElement: KafkaTopicReqImpClickMsg) = {
    false
  }

  override def getProducedType() = {
    createTypeInformation[KafkaTopicReqImpClickMsg]
      .asInstanceOf[CaseClassTypeInfo[KafkaTopicReqImpClickMsg]]
  }
}
