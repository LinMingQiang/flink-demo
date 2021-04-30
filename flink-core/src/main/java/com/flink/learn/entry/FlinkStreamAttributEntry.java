package com.flink.learn.entry;

import com.flink.common.core.EnvironmentalKey;
import com.flink.common.core.FlinkLearnPropertiesUtil;
import com.core.FlinkEvnBuilder;
import com.flink.common.deserialize.KafkaMessageDeserialize;
import com.manager.KafkaSourceManager;
import com.flink.common.kafka.KafkaManager;
import com.flink.learn.bean.ReportLogPojo;
import com.pojo.KafkaMessgePoJo;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class FlinkStreamAttributEntry {
    public static StreamExecutionEnvironment streamEnv = null;

    //        // test2: {"ts":5,"msg":"hello3"}
//        // test1 : {"ts":5,"msg":"hello2"} {"ts":10,"msg":"hello"} {"ts":115,"msg":"hello"}
//        // test3:
//        // 输出三个；
//        // test1：  {"ts":20,"msg":"hello"} 不输出 ,因为超过 10s了 。join不了test2的5s数据
//        // test2:   {"ts":10,"msg":"hello"}

    /**
     * 并行度不能太多，否则报 rocksdb
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        FlinkLearnPropertiesUtil.init(EnvironmentalKey.LOCAL_PROPERTIES_PATH(),
                "WordCountEntry");
        streamEnv = FlinkEvnBuilder.buildStreamingEnv(FlinkLearnPropertiesUtil.param(),
                FlinkLearnPropertiesUtil.CHECKPOINT_PATH(),
                FlinkLearnPropertiesUtil.CHECKPOINT_INTERVAL());
        SingleOutputStreamOperator<ReportLogPojo> req =
                KafkaSourceManager.getKafkaDataStream(streamEnv,
                        "test",
                        "localhost:9092",
                        "latest", new KafkaMessageDeserialize())
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy.<KafkaMessgePoJo>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                        .withTimestampAssigner(((element, recordTimestamp) -> element.ts)))
                        .keyBy((KeySelector<KafkaMessgePoJo, String>) value -> value.msg)
                        .process(new KeyedProcessFunction<String, KafkaMessgePoJo, ReportLogPojo>() {
                            ValueState<Boolean> has = null;
                            // 这里面的非 key state是 task公用的。这里面的Operator state也是task公用的。
                            @Override
                            public void open(Configuration parameters) throws Exception {
                                has = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("has", Types.BOOLEAN, false));
                            }

                            @Override
                            public void processElement(KafkaMessgePoJo v1, Context context, Collector<ReportLogPojo> collector) throws Exception {
                                if (!has.value()) {
                                    has.update(true);
                                    collector.collect(new ReportLogPojo(v1.msg, v1.msg, v1.ts, 0L, 0L, 1L, 0L, 0L));
                                }
                            }
                        });

        SingleOutputStreamOperator<ReportLogPojo> cd2 =
                KafkaSourceManager.getKafkaDataStream(streamEnv,
                        "test2",
                        "localhost:9092",
                        "latest", new KafkaMessageDeserialize())
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy.<KafkaMessgePoJo>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                        .withTimestampAssigner(((element, recordTimestamp) -> element.ts)))
                        .keyBy((KeySelector<KafkaMessgePoJo, String>) value -> value.msg)
                        .process(new KeyedProcessFunction<String, KafkaMessgePoJo, ReportLogPojo>() {
                            ValueState<Boolean> has = null;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                has = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("has2", Types.BOOLEAN, false));
                            }

                            @Override
                            public void processElement(KafkaMessgePoJo v1, Context context, Collector<ReportLogPojo> collector) throws Exception {
                                if (!has.value()) {
                                    has.update(true);
                                    collector.collect(new ReportLogPojo(v1.msg, v1.msg, 0L, v1.ts, 0L, 0L, 0L, 0L));
                                }
                            }
                        });


        SingleOutputStreamOperator<ReportLogPojo> cd3 =
                KafkaSourceManager.getKafkaDataStream(streamEnv,
                        "test3",
                        "localhost:9092",
                        "latest", new KafkaMessageDeserialize())
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy.<KafkaMessgePoJo>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                        .withTimestampAssigner(((element, recordTimestamp) -> element.ts)))
                        .keyBy((KeySelector<KafkaMessgePoJo, String>) value -> value.msg)
                        .process(new KeyedProcessFunction<String, KafkaMessgePoJo, ReportLogPojo>() {
                            ValueState<Boolean> has = null;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                has = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("has3", Types.BOOLEAN, false));
                            }

                            @Override
                            public void processElement(KafkaMessgePoJo v1, Context context, Collector<ReportLogPojo> collector) throws Exception {
                                if (!has.value()) {
                                    has.update(true);
                                    collector.collect(new ReportLogPojo(v1.msg, v1.msg, 0L, 0L, v1.ts, 0L, 0L, 0L));
                                }
                            }
                        });
        run(req, cd2, cd3);
        streamEnv.execute();
    }


    public static void run(SingleOutputStreamOperator<ReportLogPojo> req,
                           SingleOutputStreamOperator<ReportLogPojo> impsrc,
                           SingleOutputStreamOperator<ReportLogPojo> clicksrc) {

        SingleOutputStreamOperator<ReportLogPojo> imp = req.keyBy((KeySelector<ReportLogPojo, String>) value -> value.req_id)
                .intervalJoin(
                        impsrc.keyBy((KeySelector<ReportLogPojo, String>) value -> value.req_id))
                .between(Time.seconds(-10), Time.seconds(10)) // 前后10s
                .process(new ProcessJoinFunction<ReportLogPojo, ReportLogPojo, ReportLogPojo>() {
                    @Override
                    public void processElement(ReportLogPojo v1, ReportLogPojo v2, Context context, Collector<ReportLogPojo> collector) throws Exception {
                        if (v1.reqTime <= v2.impTime) {
                            collector.collect(new ReportLogPojo(v1.req_id, v1.app_id, v1.reqTime, v2.impTime, 0L, 0L, 1L, 0L));
                        }
                    }
                });

        SingleOutputStreamOperator<ReportLogPojo> click = imp.keyBy((KeySelector<ReportLogPojo, String>) v -> v.req_id)
                .intervalJoin(
                        clicksrc.keyBy((KeySelector<ReportLogPojo, String>) value -> value.req_id))
                .between(Time.seconds(-10), Time.seconds(10))
                .process(new ProcessJoinFunction<ReportLogPojo, ReportLogPojo, ReportLogPojo>() {
                    @Override
                    public void processElement(ReportLogPojo imp, ReportLogPojo click, Context context, Collector<ReportLogPojo> collector) throws Exception {
                        if (click.clickTime >= imp.impTime) {
                            collector.collect(new ReportLogPojo(imp.req_id, imp.app_id, imp.reqTime, imp.impTime, click.clickTime, 0L, 0L, 1L));
                        }
                    }
                });


        req
                .map(x -> new Tuple2<>(x.reqTime / 1000L, x.req))
                .returns(Types.TUPLE(Types.LONG, Types.LONG))
                .keyBy(x -> x.f0)
                .sum(1)
                .print();


        imp.map(x -> new Tuple2<>(x.reqTime / 1000L, x.imp))
                .returns(Types.TUPLE(Types.LONG, Types.LONG))
                .keyBy(x -> x.f0)
                .sum(1)
                .print();


        click.map(x -> new Tuple2<>(x.reqTime / 1000L, x.click))
                .returns(Types.TUPLE(Types.LONG, Types.LONG))
                .keyBy(x -> x.f0)
                .sum(1)
                .print();

    }
}
