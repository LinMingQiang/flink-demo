package com.batch.sql.scala.test

import com.flink.learn.test.common.FlinkStreamTableCommonSuit
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.types.Row
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
class FlinkLearnDataSetSQLEntry extends FlinkStreamTableCommonSuit{
  def testFactory(): Unit = {
//    val streamEnv = FlinkEvnBuilder.buildStreamingEnv(
//      FlinkLearnPropertiesUtil.param,
//      FlinkLearnPropertiesUtil.CHECKPOINT_PATH,
//      FlinkLearnPropertiesUtil.CHECKPOINT_INTERVAL)
//    val tableEnv = FlinkEvnBuilder.buildStreamTableEnv(streamEnv,
//                                                       Time.minutes(1),
//                                                       Time.minutes(6))
//    val a = tableEnv.fromDataStream(
//      streamEnv.addSource(
//        FlinkJavaStreamTableTestBase
//          .getKafkaSource("test", "localhost:9092", "latest")),
//      "topic,offset,msg").renameColumns("offset as ll"); // offset是关键字
//    tableEnv.sqlUpdate(DDLSourceSQLManager.createCustomSinkTbl("printlnSinkTbl"))
//
//    tableEnv.insertInto("printlnSinkTbl", a.select("topic,ll,msg"))
//
//
//    tableEnv.execute("eee")
  }

 test("wordcounttest"){
    // val input = env.fromElements(WC("hello", 1), WC("hello", 1), WC("ciao", 1))
    val input =
      bEnv.fromCollection(Array(WC("hello", 1), WC("hello", 1), WC("ciao", 1)))
    // register the DataSet as table "WordCount"
    batchEnv.registerDataSet("WordCount", input, 'word, 'frequency)
    // run a SQL query on the Table and retrieve the result as a new Table
    val table =
      batchEnv.sqlQuery("SELECT word, SUM(frequency) FROM WordCount GROUP BY word")
    // table.toDataSet[WC].print()
    table.toDataSet[Row].print()
  }

  // *************************************************************************
  //     USER DATA TYPES
  // *************************************************************************
  case class WC(word: String, frequency: Long)

}
