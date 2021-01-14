package com.flink.test;

import com.flink.common.java.core.FlinkSourceBuilder;
import com.flink.common.kafka.KafkaManager;
import com.flink.common.kafka.KafkaManager.*;
import com.flink.learn.test.common.FlinkJavaStreamTableTestBase;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

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
        Pattern<KafkaTopicOffsetTimeMsg, KafkaTopicOffsetTimeMsg> req_imp =
                Pattern.<KafkaTopicOffsetTimeMsg>begin("start")
                        .where(new SimpleCondition<KafkaTopicOffsetTimeMsg>() {
                            @Override
                            public boolean filter(KafkaTopicOffsetTimeMsg kafkaTopicOffsetTimeMsg) throws Exception {
                                return kafkaTopicOffsetTimeMsg.msg().equals("start");
                            }
                        })
                        .next("middle") // 接着有imp
                        .where(new SimpleCondition<KafkaTopicOffsetTimeMsg>() {
                            @Override
                            public boolean filter(KafkaTopicOffsetTimeMsg kafkaTopicOffsetTimeMsg) throws Exception {
                                return kafkaTopicOffsetTimeMsg.msg().equals("middle");
                            }
                        })
                        .within(Time.seconds(40)) // 取决于start后  40s内 (如果这个失效，后面都失效)
                        .followedByAny("end") // 接着有imp
                        .where(new SimpleCondition<KafkaTopicOffsetTimeMsg>() {
                            @Override
                            public boolean filter(KafkaTopicOffsetTimeMsg kafkaTopicOffsetTimeMsg) throws Exception {
                                return kafkaTopicOffsetTimeMsg.msg().equals("end");
                            }
                        })
                        .within(Time.seconds(10)) // 取决于 middle的时间 + 10s  内。触发后可以连续输入，但是如果middle失效了（40s），那这个也失效了
                        .or(new SimpleCondition<KafkaTopicOffsetTimeMsg>() {
                            @Override
                            public boolean filter(KafkaTopicOffsetTimeMsg kafkaTopicOffsetTimeMsg) throws Exception {
                                return kafkaTopicOffsetTimeMsg.msg().equals("noend");
                            }
                        })
                        .within(Time.seconds(40));
        CEP
                .pattern(baseEventtimeJsonSource.keyBy(x -> x.topic()), req_imp)
                .inProcessingTime() // 每来一条就触发，eventtime的话就是更加watermark来触发
                .select(new PatternSelectFunction<KafkaTopicOffsetTimeMsg, String>() {
                    @Override
                    public String select(Map<String, List<KafkaTopicOffsetTimeMsg>> map) throws Exception {
                        return map.toString();
                    }
                })
                .print();

        streamEnv.execute();
    }



    @Test
    public void testUntil(){

    }

    @Test
    public void testOutputTag(){

    }
}
