package com.flink.common.core

object CaseClassManager {
  case class Order(user: Long, product: String, amount: Int)

  case class KafkaMsg(topic: String, msg: String)
}
