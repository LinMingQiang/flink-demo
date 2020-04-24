package com.flink.learn.sql.batch.entry

import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, StreamQueryConfig}
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
object FlinkLearnDataStreamSQLEntry {
  def main(args: Array[String]): Unit = {

    // set up execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val sett =
      EnvironmentSettings.newInstance.useBlinkPlanner.inStreamingMode.build
    val tEnv = StreamTableEnvironment.create(env, sett)

    // 创建两个流 A
    val orderA: DataStream[Order] = env.fromCollection(
      Seq(Order(1L, "beer", 3), Order(1L, "diaper", 4), Order(3L, "rubber", 2)))
    // 创建两个流 B
    val orderB: DataStream[Order] = env.fromCollection(
      Seq(Order(2L, "pen", 3), Order(2L, "rubber", 3), Order(4L, "beer", 1)))

    // 两种注册成 Table的方法
    // convert DataStream to Table
    var tableA = tEnv.fromDataStream(orderA, 'user, 'product, 'amount)
    // register DataStream as Table
    tEnv.registerDataStream("OrderB", orderB, 'user, 'product, 'amount)

    // union the two tables
    val result = tEnv.sqlQuery(
      s"SELECT * FROM $tableA WHERE amount > 2 UNION ALL " +
        "SELECT * FROM OrderB WHERE amount < 2")
    // state 保存时间 minTime maxTime
    val queryConfig = new StreamQueryConfig().withIdleStateRetentionTime(Time.hours(1), Time.hours(2))

    // sql 结果转为 streamDataset
    tEnv.toAppendStream[Order](result, queryConfig).print()
    // result.toAppendStream[Order].print()
    env.execute()
  }
  case class Order(user: Long, product: String, amount: Int)
}
