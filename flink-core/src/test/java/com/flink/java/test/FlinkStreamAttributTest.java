package com.flink.java.test;

import com.flink.common.kafka.KafkaManager;

import com.flink.learn.bean.ReportLogPojo;
import com.flink.learn.test.common.FlinkJavaStreamTableTestBase;
import com.pojo.KafkaMessgePoJo;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.junit.Test;

public class FlinkStreamAttributTest extends FlinkJavaStreamTableTestBase {
    /**
     * 归因， 请求 -》 曝光 -》 点击
     * 对日志数据进行去重。
     */
    @Test
    public void testAttribute() throws Exception {
        // test2: {"ts":5,"msg":"hello2"}
        // test1 : {"ts":5,"msg":"hello2"} {"ts":10,"msg":"hello"} {"ts":15,"msg":"hello"}
        // test3:
        // 输出三个；
        // test1：  {"ts":20,"msg":"hello"} 不输出 ,因为超过 10s了 。join不了test2的5s数据
        // test2:   {"ts":10,"msg":"hello"}
        // 输出 ： 四个 0-20 的都会再输出一遍。
        // v2 是曝光，v1是请求，v3是点击
        SingleOutputStreamOperator<ReportLogPojo> req =
                getKafkaKeyStream("test", "localhost:9092", "latest")
                        .process(new KeyedProcessFunction<String, KafkaMessgePoJo, ReportLogPojo>() {
                            ValueState<Boolean> has = null;

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

        SingleOutputStreamOperator<ReportLogPojo> imp = req.keyBy((KeySelector<ReportLogPojo, String>) value -> value.req_id)
                .intervalJoin(
                        getKafkaKeyStream("test2", "localhost:9092", "latest")
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
                                })
                                .keyBy((KeySelector<ReportLogPojo, String>) value -> value.req_id))
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
                        getKafkaKeyStream("test3", "localhost:9092", "latest")
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
                                }).keyBy((KeySelector<ReportLogPojo, String>) value -> value.req_id))
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
        streamEnv.execute();
    }
}
