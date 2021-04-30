package com.func.richfunc

import com.pojo.KafkaMessgePoJo
import org.apache.flink.streaming.api.scala.async.{ResultFuture, RichAsyncFunction}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class AsyncIORichFunction
    extends RichAsyncFunction[
      KafkaMessgePoJo,
     Tuple2[KafkaMessgePoJo, KafkaMessgePoJo]] {

  override def asyncInvoke(
      input: KafkaMessgePoJo,
      resultFuture: ResultFuture[(KafkaMessgePoJo, KafkaMessgePoJo)]): Unit = {
    // 发送异步请求，接收 future 结果
    val resultFutureRequested: Future[KafkaMessgePoJo] = Future {
      Thread.sleep(4000L)
      input.setMsg("")
      input
    }

    // 设置客户端完成请求后要执行的回调函数
    // 回调函数只是简单地把结果发给 future
    resultFutureRequested.onSuccess {
      case result: KafkaMessgePoJo =>
        resultFuture.complete(Iterable((input, result)))
    }


  }
}
