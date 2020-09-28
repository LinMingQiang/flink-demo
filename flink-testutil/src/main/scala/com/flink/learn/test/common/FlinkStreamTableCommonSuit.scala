package com.flink.learn.test.common

import com.flink.common.core.{EnvironmentalKey, FlinkEvnBuilder, FlinkLearnPropertiesUtil}
import com.flink.common.core.FlinkLearnPropertiesUtil.{CHECKPOINT_PATH, param}
import com.flink.common.deserialize.TopicOffsetMsgDeserialize
import com.flink.common.kafka.KafkaManager
import com.flink.common.kafka.KafkaManager.KafkaTopicOffsetMsg
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala.{BatchTableEnvironment, StreamTableEnvironment}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class FlinkStreamTableCommonSuit extends FunSuite with BeforeAndAfterAll {
  var tableEnv: StreamTableEnvironment = null
  var tableE: TableEnvironment = null
  var streamEnv: StreamExecutionEnvironment = null
  var bEnv: ExecutionEnvironment = null
  var batchEnv: BatchTableEnvironment = null

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    FlinkLearnPropertiesUtil.init(EnvironmentalKey.LOCAL_PROPERTIES_PATH,
                                  "FlinkLearnStreamDDLSQLEntry")
    tableEnv = FlinkEvnBuilder.buildStreamTableEnv(param,
                                                   CHECKPOINT_PATH,
                                                   10000,
                                                   Time.minutes(1),
                                                   Time.minutes(6))
    System.out.println("! INITIALIZE ExecutionEnvironment SUCCESS !")
    FlinkLearnPropertiesUtil.init(EnvironmentalKey.LOCAL_PROPERTIES_PATH,
                                  "FlinkLearnStreamDDLSQLEntry")
    bEnv = ExecutionEnvironment.createLocalEnvironment()
    batchEnv = BatchTableEnvironment.create(bEnv)
    streamEnv = FlinkEvnBuilder.buildStreamingEnv(
      FlinkLearnPropertiesUtil.param,
      FlinkLearnPropertiesUtil.CHECKPOINT_PATH,
      FlinkLearnPropertiesUtil.CHECKPOINT_INTERVAL)

    tableEnv = FlinkEvnBuilder.buildStreamTableEnv(
      FlinkLearnPropertiesUtil.param,
      FlinkLearnPropertiesUtil.CHECKPOINT_PATH,
      FlinkLearnPropertiesUtil.CHECKPOINT_INTERVAL,
      Time.minutes(1),
      Time.minutes(6))
  }

  protected override def afterAll(): Unit = {
    super.afterAll()
  }

  /**
    *
    * @param topic
    * @param broker
    * @return
    */
  def kafkaSource(
      topic: String,
      broker: String): FlinkKafkaConsumer010[KafkaTopicOffsetMsg] = {
    val kafkasource = KafkaManager.getKafkaSource(
      topic,
      broker,
      new TopicOffsetMsgDeserialize())
    kafkasource.setCommitOffsetsOnCheckpoints(true)
    kafkasource.setStartFromEarliest() //不加这个默认是从上次消费
    kafkasource
  }


}
