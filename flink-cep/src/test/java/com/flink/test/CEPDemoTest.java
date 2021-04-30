package com.flink.test;

import com.flink.common.kafka.KafkaManager.*;
import com.flink.learn.test.common.FlinkJavaStreamTableTestBase;
import com.pojo.KafkaMessgePoJo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class CEPDemoTest extends FlinkJavaStreamTableTestBase {

    /**
     * 注意 使用 keyStream ： 因为 Pattern 是应用在各个分区的，所以keyby之后注意同一个链路是否在一个分区里面，
     * 例如这个例子不能用 msg做keyby，否则start 和 middle在不同分区
     * @throws Exception
     */
    @Test
    public void testDemo() throws Exception {
        //  {"ts":1,"msg":"start"}
        //  {"ts":2,"msg":"middle"}
        //  {"ts":4,"msg":"end"}
        // {"ts":4,"msg":"noend"}
        Pattern<KafkaMessgePoJo, KafkaMessgePoJo> req_imp =
                Pattern.<KafkaMessgePoJo>begin("start")
                        .where(new SimpleCondition<KafkaMessgePoJo>() {
                            @Override
                            public boolean filter(KafkaMessgePoJo KafkaMessgePoJo) throws Exception {
                                return KafkaMessgePoJo.msg.equals("start");
                            }
                        })
                        .next("middle") // 接着有imp
                        .where(new SimpleCondition<KafkaMessgePoJo>() {
                            @Override
                            public boolean filter(KafkaMessgePoJo KafkaMessgePoJo) throws Exception {
                                return KafkaMessgePoJo.msg.equals("middle");
                            }
                        })
                        .within(Time.seconds(40)) // 取决于start后  40s内 (如果这个失效，后面都失效)
                        .followedByAny("end") // 接着有imp
                        .where(new SimpleCondition<KafkaMessgePoJo>() {
                            @Override
                            public boolean filter(KafkaMessgePoJo KafkaMessgePoJo) throws Exception {
                                return KafkaMessgePoJo.msg.equals("end");
                            }
                        })
                        .within(Time.seconds(10)) // 取决于 middle的时间 + 10s  内。触发后可以连续输入，但是如果middle失效了（40s），那这个也失效了
                        .or(new SimpleCondition<KafkaMessgePoJo>() {
                            @Override
                            public boolean filter(KafkaMessgePoJo KafkaMessgePoJo) throws Exception {
                                return KafkaMessgePoJo.msg.equals("noend");
                            }
                        })
                        .within(Time.seconds(40));
        // baseEventtimeJsonSource.keyBy(x -> x.topic()) 这个地方要注意，
        // 如果是用msg做keyby，因为规则里面都是用msg。keyby会导致数据分组不同分区，规则也就不符合了
        // 所以最好是用你报表的key做分区，
        CEP
                .pattern(getKafkaDataStreamSource("", "", "").keyBy((KeySelector<KafkaMessgePoJo, String>) value -> value.topic), req_imp)
                .inProcessingTime() // 每来一条就触发，eventtime的话就是更加watermark来触发
                .select(new PatternSelectFunction<KafkaMessgePoJo, String>() {
                    @Override
                    public String select(Map<String, List<KafkaMessgePoJo>> map) throws Exception {
                        return map.toString();
                    }
                })
                .print();

        streamEnv.execute();
    }



    @Test
    public void testUntil() throws Exception {
        // {"ts":10,"msg":"1"} {"ts":10,"msg":"8"}
        Pattern<KafkaMessgePoJo, KafkaMessgePoJo> pattern = Pattern.<KafkaMessgePoJo>begin("start").where(
                new SimpleCondition<KafkaMessgePoJo>() {
                    @Override
                    public boolean filter(KafkaMessgePoJo event) {
                        boolean r = event.msg.equals("1");
                        return r;
                    }
                }
        ).next("middle").where(
                new SimpleCondition<KafkaMessgePoJo>() {
                    @Override
                    public boolean filter(KafkaMessgePoJo event) {
                        boolean r = event.msg.equals("8");
                        return r;
                    }
                }
        );

        CEP.pattern(kafkaDataSource, pattern)
                .inProcessingTime() // 每来一条就触发，eventtime的话就是更加watermark来触发
                .select(new PatternSelectFunction<KafkaMessgePoJo, String>() {
            @Override
            public String select(Map<String, List<KafkaMessgePoJo>> p) throws Exception {
                return p.toString();
            }
        }).print();

        streamEnv.execute("flink learning cep");
    }

    @Test
    public void testOutputTag(){

    }
}
