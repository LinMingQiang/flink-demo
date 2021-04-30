package com.flink.java.test;

import com.pojo.KafkaMessgePoJo;
import com.pojo.WordCountPoJo;
import com.func.processfunc.StreamConnectCoProcessFunc;
import com.func.richfunc.AsyncIODatabaseRequest;
import com.flink.learn.test.common.FlinkJavaStreamTableTestBase;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.junit.Test;

import java.util.concurrent.TimeUnit;


/**
 * 基本算子的使用
 */
public class FlinkCoreOperatorTest extends FlinkJavaStreamTableTestBase {

    /**
     * wordcount
     *
     * @throws Exception
     */
    @Test
    public void testWordCount() throws Exception {
        // {"msg":"hello"}
        kafkaDataSource
                .flatMap((FlatMapFunction<KafkaMessgePoJo, String>) (value, out) -> {
                    for (String s : value.msg.split(",", -1)) {
                        out.collect(s);
                    }
                })
                .returns(Types.STRING)
                .map(x -> new Tuple2(x, 1L))
                .setParallelism(1)
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(x -> x.f0)
                 .sum(1)
                 .setParallelism(1)
                .print();

        streamEnv.execute("lmq-flink-demo"); //程序名
    }

    /**
     * 侧边输出，将过滤的数据从另外一个sink输出
     */
    @Test
    public void testOutputTag() throws Exception {
        OutputTag<WordCountPoJo> rejectedWordsTag = new OutputTag<WordCountPoJo>("rejected") {
        };
        SingleOutputStreamOperator<WordCountPoJo> sourceStream = kafkaDataSource
                .process(new KeyedProcessFunction<String, KafkaMessgePoJo, WordCountPoJo>() {
                    @Override
                    public void processElement(KafkaMessgePoJo value, Context ctx, Collector<WordCountPoJo> out) {
                        if (!value.msg.matches("^[0-9]*$")) {
                            ctx.output(rejectedWordsTag, new WordCountPoJo("rejested", 1));
                        } else {
                            out.collect(new WordCountPoJo(value.msg, 1));
                        }
                    }
                })
                .returns(WordCountPoJo.class);
        // 正常的数字输出
        sourceStream.keyBy(x -> x.word)
                .sum("num")
                .print();
        // 错误数据输出
        sourceStream
                .getSideOutput(rejectedWordsTag)
                .keyBy(x -> x.word)
                .sum("num")
                .print();
        System.out.println(streamEnv.getExecutionPlan());
        streamEnv.execute("testOutputTag");
    }

    /**
     * watermark是广播发送的，watermark是广播发送的，watermark是广播发送的
     * 可用于双流join。假设 Test2为维表（永久保存状态）
     * 双流connect 必须都有watermark，否则一个产生不了watermark，不会触发 registerEventimeTimer
     * 输入a: a1 a2 a3 ，a流的wtm = a3 。但是因为是双流 ，所以wtm取最小的 = b = min
     * 再输入b : b2 b3 ， b流的wtm = b3 。  最后的wtm = min (a3, b3) = (a3 - 10s)
     * 这个时候才触发 a的过期，a1,a2 ，如果已经超过10s了的话
     * 后面再输入其他的，就去 最小的那个。每次输入，各自流都会更新自己的wtm，然后再跟另一个比较取最小
     *
     * @throws Exception
     */
    @Test
    public void testConnectStream() throws Exception {
        // 10s过期
        OutputTag<String> rejectedWordsTag = new OutputTag<String>("rejected") {
        };

        SingleOutputStreamOperator resultStream =
                kafkaDataSource
                        .connect(getKafkaKeyStream("test2", "localhost:9092", "latest"))
                        .keyBy(KafkaMessgePoJo::getMsg, KafkaMessgePoJo::getMsg)
                        .process(new StreamConnectCoProcessFunc(rejectedWordsTag))
                        .setParallelism(4);


        resultStream.returns(Types.STRING).print();
        resultStream.getSideOutput(rejectedWordsTag).print();

        streamEnv.execute("");
    }

    /**
     * 异步io测试：这玩意挺废的？必须客户端连接是异步的。本身 task不是异步方式调用asyncInvoke
     * 除非 asyncInvoke(...) 方法快速返回并且依赖于（客户端的）回调, 否则无法实现正确的异步 I/O。
     * 例如，
     * 以下情况导致阻塞的 asyncInvoke(...) 函数，从而使异步行为无效：
     * 使用同步数据库客户端，它的查询方法调用在返回结果前一直被阻塞。
     * 在 asyncInvoke(...) 方法内阻塞等待异步客户端返回的 future 类型对象
     */
    @Test
    public void testAsyncIo() throws Exception {
        AsyncDataStream.unorderedWait(
                kafkaDataSource,
                new AsyncIODatabaseRequest(),
                4,
                TimeUnit.SECONDS,
                3) // 100异步最大个数，超过100个请求将构成反压。
                .print();
        streamEnv.execute("lmq-flink-demo"); //程序名
    }
}
