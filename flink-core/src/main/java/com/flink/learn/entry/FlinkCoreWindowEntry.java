package com.flink.learn.entry;

import com.flink.common.core.EnvironmentalKey;
import com.flink.common.core.FlinkLearnPropertiesUtil;
import com.flink.common.deserialize.KafkaMessageDeserialize;
import com.core.FlinkEvnBuilder;
import com.manager.KafkaSourceManager;
import com.pojo.KafkaMessgePoJo;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class FlinkCoreWindowEntry {
    public static StreamExecutionEnvironment streamEnv = null ;
    public static void main(String[] args) throws Exception {
        FlinkLearnPropertiesUtil.init(EnvironmentalKey.LOCAL_PROPERTIES_PATH(),
                "WordCountEntry");
        streamEnv = FlinkEvnBuilder.buildStreamingEnv(FlinkLearnPropertiesUtil.param(),
                FlinkLearnPropertiesUtil.CHECKPOINT_PATH(),
                10000L);
//        StreamTableEnvironment tableEnv = FlinkEvnBuilder.buildStreamTableEnv(
//                streamEnv,
//                Time.minutes(1),
//                Time.minutes(6));
        switch (args[0]) {
            case "testWindow":
                testWindow();        }

    }

    /**
     * 并发情况下，所有的并行slot上的watermark都是同步的？所以同一个时间段的window都是同时触发的，
     * 例如keyby之后 并行度为3， slot的wtm分别为1，3，3。 windowMaxtime = 2 。
     * 那window还是会触发，从webiu上看，他的watermark是3
     * @throws Exception
     */
    public static void testWindow() throws Exception {
        KafkaSourceManager.getKafkaDataStream(streamEnv,
                "test",
                "localhost:9092",
                "latest", new KafkaMessageDeserialize())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<KafkaMessgePoJo>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                .withTimestampAssigner(((element, recordTimestamp) -> element.ts)))
                .returns(KafkaMessgePoJo.class)
                .map((MapFunction<KafkaMessgePoJo, Tuple2<String, Long>>) value -> new Tuple2<>(value.msg, 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .setParallelism(2)
                .keyBy((KeySelector<Tuple2<String, Long>, String>) o -> o.f0 )
                .window(TumblingEventTimeWindows.of(Time.seconds(5))) // 统计5s一个窗口
                .process(new ProcessWindowFunction<Tuple2<String, Long>,
                        String,
                        Tuple1<String>,
                        TimeWindow>() {
                    @Override
                    public void process(Tuple1<String> s,
                                        Context context,
                                        Iterable<Tuple2<String, Long>> elements,
                                        Collector<String> out) throws Exception {
                        long count = 0;
                        for (Tuple2<String, Long> in: elements) {
                            count++;
                        }
                        out.collect("Window: " + context.window() + "count: " + count);
                    }
                })
//                 .sum(1)
                .setParallelism(2)
                .print();

        streamEnv.execute("lmq-flink-demo"); //程序名

    }
}
