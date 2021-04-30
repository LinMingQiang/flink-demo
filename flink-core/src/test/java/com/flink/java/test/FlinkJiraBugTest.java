package com.flink.java.test;

import com.flink.common.kafka.KafkaManager;
import com.flink.learn.test.common.FlinkJavaStreamTableTestBase;
import com.pojo.KafkaMessgePoJo;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.junit.Test;

public class FlinkJiraBugTest extends FlinkJavaStreamTableTestBase {

    @Test
    public void testWindow() throws Exception {
        // {"ts":10,"msg":"c"} {"ts":11,"msg":"c"} {"ts":150,"msg":"c"}
        kafkaDataSource
                .map((MapFunction<KafkaMessgePoJo, Tuple2<String, Long>>) value -> new Tuple2<>(value.msg, 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy((KeySelector<Tuple2<String, Long>, String>) o -> o.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(100)))
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(5)))
                .reduce((ReduceFunction<Tuple2<String, Long>>) (value1, value2) -> new Tuple2<String, Long>(value1.f0, value2.f1 + value1.f1)
                        , new ProcessWindowFunction<Tuple2<String, Long>,
                                Tuple3<TimeWindow, String, Long>,
                                String,
                                TimeWindow>() {
                            @Override
                            public void process(String s,
                                                Context context,
                                                Iterable<Tuple2<String, Long>> elements,
                                                Collector<Tuple3<TimeWindow, String, Long>> out) throws Exception {
                                Long count = 0L;
                                for (Tuple2<String, Long> element : elements) {
                                    count += element.f1;
                                }
                                out.collect(new Tuple3(context.window(), s, count));
                            }
                        })
//                 .sum(1)
                .print();
        streamEnv.execute("lmq-flink-demo"); //程序名
    }
}
