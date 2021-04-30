package com.flink.java.test;

import com.manager.KafkaSourceManager;
import com.flink.common.kafka.KafkaManager;
import com.flink.learn.test.common.FlinkJavaStreamTableTestBase;
import com.pojo.KafkaMessgePoJo;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.junit.Test;
public class FlinkCoreWindowTest extends FlinkJavaStreamTableTestBase {

    /**
     * window 有两种 Windowfunction ：
     * AggregateFunction(一条一条聚合)
     * ProcessindowFunction 触发的时候一次性聚合， Iterator给你数据
     */
    @Test
    public void testWindow() throws Exception {
        // {"rowtime":"2020-01-01 00:00:00","msg":"c"} {"rowtime":"2020-01-01 00:00:01","msg":"c"}
        // {"rowtime":"2020-01-01 00:00:32","msg":"c"}
        // {"rowtime":"2020-01-01 00:01:40","msg":"c"}
        kafkaDataSource
                .map((MapFunction<KafkaMessgePoJo, Tuple2<String, Long>>) value -> new Tuple2<>(value.msg, 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy((KeySelector<Tuple2<String, Long>, String>) o -> o.f0)
                // 统计5s一个窗口，有个offset参数，用来调整时间起点，正常是00-05这样，可以调成 01-06
                .window(TumblingEventTimeWindows.of(Time.seconds(20)))
                // 固定时间触发, 每10s触发一次(系统时间) .如果没有设置，则是根据eventtime > window end time 来决定触发
                // 如果定义了trigger，会每次触发sum操作。最好的方法就是定期清理窗口的数据，不然每次触发都是拿窗口的全部数据做计算
                // 不管有没有数据都会触发，会重复输出之前的结果
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(5)))
                // .trigger(EventTimeTrigger.create()) // 以eventtime 时间触发窗口，当wartermark 》 window endtime 触发

                // evictor 和 trigger 一起用会导致窗口算的结果不对 （因为evictor把数据清了，trigger是需要窗口数据做计算），
                // 除非用process把中间算的结果存起来，参考 FlinkStreamDAUTest,或者用reduce，不能用process

//               .evictor(TimeEvictor.of(Time.seconds(0), true)) // 只保留 窗口内最近2s的数据做计算
                // 在process才产生 Transformation，上面的定义存在 WindowOperatorBuilder 里面
                // 如果使用了 evictor，reduce的优势就不存在了，因为数据要全量保存下来然后再reduce。看 ReduceApplyProcessWindowFunction的process
                // 如果没用用evictor，reduce会变成一个 WindowState:RocksDBReducingState。 WindowState.add(element)调用了reducefunc
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


    @Test
    public void testWindowAll() throws Exception {
        kafkaDataSource
                .map((MapFunction<KafkaMessgePoJo, Tuple2<String, Long>>) value -> new Tuple2<>(value.msg, 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new ProcessAllWindowFunction<Tuple2<String, Long>, String, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
                        int c = 0;
                        for (Tuple2<String, Long> element : elements) {
                            c++;
                        }
                        out.collect(context.window() + " : " + c);
                    }
                })
                // .setParallelism(4) // 不可设置，并行度必须为1
                .print();
        streamEnv.execute();
    }
}
