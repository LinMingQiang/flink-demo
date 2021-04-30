package com.func.richfunc;

import com.flink.common.core.FlinkLearnPropertiesUtil;
import com.flink.common.dbutil.FlinkHbaseFactory;
import com.pojo.KafkaMessgePoJo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.hadoop.hbase.client.Connection;

import java.util.Collections;
import java.util.concurrent.*;
import java.util.function.Supplier;

public class AsyncIODatabaseRequest extends RichAsyncFunction<KafkaMessgePoJo,
        Tuple2<KafkaMessgePoJo, KafkaMessgePoJo>> {
    Connection conn = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        ParameterTool parame = (ParameterTool) getRuntimeContext()
                .getExecutionConfig()
                .getGlobalJobParameters();
        FlinkLearnPropertiesUtil.init(parame);
        FlinkHbaseFactory.getGlobalConn(FlinkLearnPropertiesUtil.ZOOKEEPER());
    }

    @Override
    public void asyncInvoke(KafkaMessgePoJo input,
                            ResultFuture<Tuple2<KafkaMessgePoJo, KafkaMessgePoJo>> resultFuture) throws Exception {
        final Future<KafkaMessgePoJo> result = queryClient(input);
        // 设置客户端完成请求后要执行的回调函数
        // 回调函数只是简单地把结果发给 future
        CompletableFuture.supplyAsync(new Supplier<KafkaMessgePoJo>() {
            @Override
            public KafkaMessgePoJo get() {
                try {
                    return result.get();
                } catch (InterruptedException | ExecutionException e) {
                    // 显示地处理异常。
                    return null;
                }
            }
        }).thenAccept((KafkaMessgePoJo dbResult) -> {
            resultFuture.complete(Collections.singleton(new Tuple2<>(input, dbResult)));
        });
    }

    /**
     * 释放过期，否则超过个数导致反压
     *
     * @param input
     * @param resultFuture
     * @throws Exception
     */
    @Override
    public void timeout(KafkaMessgePoJo input,
                        ResultFuture<Tuple2<KafkaMessgePoJo, KafkaMessgePoJo>> resultFuture) throws Exception {
        resultFuture.complete(Collections.singleton(new Tuple2<>(input,
                new KafkaMessgePoJo(
                        input.topic,
                        input.offset,
                        0L,
                        "timeout",
                        null,
                        null))));
    }

    public Future<KafkaMessgePoJo> queryClient(KafkaMessgePoJo input) {
        return new Future<KafkaMessgePoJo>() {
            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                return false;
            }

            @Override
            public boolean isCancelled() {
                return false;
            }

            @Override
            public boolean isDone() {
                return false;
            }

            @Override
            public KafkaMessgePoJo get() {
                if (input.msg.equals("1")) {
                    // FlinkHbaseFactory.get(conn, "test", input.msg());

                    return new KafkaMessgePoJo(
                            input.topic,
                            input.offset,
                            0L,
                            "hello",
                            null,
                            null);
                } else {
                    return input;
                }
            }

            @Override
            public KafkaMessgePoJo get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
                return get();
            }
        };
    }
}
