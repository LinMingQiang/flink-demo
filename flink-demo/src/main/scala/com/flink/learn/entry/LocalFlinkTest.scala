package com.flink.learn.entry

import org.apache.flink.streaming.api.scala._
import com.flink.learn.bean.{AdlogBean, StatisticalIndic}
import com.flink.common.core.{
  EnvironmentalKey,
  FlinkEvnBuilder,
  FlinkLearnPropertiesUtil
}
import com.flink.common.core.FlinkLearnPropertiesUtil._
import com.flink.common.deserialize.TopicMessageDeserialize
import com.flink.common.kafka.KafkaManager
import com.flink.common.kafka.KafkaManager.KafkaMessge
import com.flink.learn.richf.AdlogPVRichFlatMapFunction
import com.flink.learn.sink.StateRecoverySinkCheckpointFunc
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

import scala.collection.JavaConversions._
object LocalFlinkTest {

  /**
    * @author LMQ
    * @version 1.8.0
    * @param args
    */
  def main(args: Array[String]): Unit = {
    FlinkLearnPropertiesUtil.init(EnvironmentalKey.LOCAL_PROPERTIES_PATH,
                                  "LocalFlinkTest")
    val kafkasource = new FlinkKafkaConsumer010[KafkaMessge](
      TEST_TOPIC.split(",").toList,
      new TopicMessageDeserialize(),
      KafkaManager.getKafkaParam(BROKER))

    kafkasource.setCommitOffsetsOnCheckpoints(true)
    kafkasource.setStartFromLatest() //不加这个默认是从上次消费
//    kafkasource.setStartFromSpecificOffsets(
//      Map(new KafkaTopicPartition("maxwell_new", 0) -> 1L.asInstanceOf[java.lang.Long]));

    val env = FlinkEvnBuilder.buildStreamingEnv(param,
                                                FLINK_DEMO_CHECKPOINT_PATH,
                                                60000) // 1 min
    val result = env
      .addSource(kafkasource)
      .map { x =>
        val datas = x.msg.split(",")
        val statdate = datas(0).substring(0, 10) //日期
        val hour = datas(0).substring(11, 13) //hour
        val plan = datas(25)
        if (plan.nonEmpty) {
          new AdlogBean(plan, statdate, hour, StatisticalIndic(1))
        } else null
      }
      .filter { x =>
        x != null
      }
      .keyBy(_.key) //按key分组，可以把key相同的发往同一个slot处理
      .flatMap(new AdlogPVRichFlatMapFunction) //通常都是用的flatmap，功能类似 (filter + map)

    //operate state。用于写hbase是吧恢复
    result.addSink(new StateRecoverySinkCheckpointFunc(50))
    //多个sink输出
    //result.addSink(new SystemPrintSink)
    //result.addSink(new HbaseReportSink)

    env.execute("lmq-flink-demo") //程序名
  }

}
