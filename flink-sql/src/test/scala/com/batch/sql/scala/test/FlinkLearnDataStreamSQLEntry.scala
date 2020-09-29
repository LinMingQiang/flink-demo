package com.batch.sql.scala.test

import com.flink.common.core.CaseClassManager.Order
import com.flink.learn.test.common.FlinkStreamTableCommonSuit
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
class FlinkLearnDataStreamSQLEntry  extends FlinkStreamTableCommonSuit{
  test(""){
    // 创建两个流 A
    val orderA: DataStream[Order] = streamEnv.fromCollection(
      Seq(Order(1L, "beer", 3), Order(1L, "diaper", 4), Order(3L, "rubber", 2)))
    // 创建两个流 B
    val orderB: DataStream[Order] = streamEnv.fromCollection(
      Seq(Order(2L, "pen", 3), Order(2L, "rubber", 3), Order(4L, "beer", 1)))

    // 两种注册成 Table的方法
    // convert DataStream to Table
    var tableA = tableEnv.fromDataStream(orderA, 'user, 'product, 'amount)
    // register DataStream as Table
    tableEnv.registerDataStream("OrderB", orderB, 'user, 'product, 'amount)
    // union the two tables
    val result = tableEnv.sqlQuery(
      s"SELECT * FROM $tableA WHERE amount > 2 UNION ALL " +
        "SELECT * FROM OrderB WHERE amount < 2")
    // state 保存时间 minTime maxTime
    tableEnv.getConfig
      .setIdleStateRetentionTime(Time.hours(1), Time.hours(2))
    // sql 结果转为 streamDataset
    tableEnv.toRetractStream[Order](result).print()
    // tEnv.toAppendStream[Order](result, queryConfig).print()
    // result.toAppendStream[Order].print()
    tableEnv.execute("")
  }
}
