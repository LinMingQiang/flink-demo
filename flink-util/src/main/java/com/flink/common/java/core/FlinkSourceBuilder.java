package com.flink.common.java.core;

import com.flink.common.core.EnvironmentalKey;
import com.flink.common.core.FlinkLearnPropertiesUtil;
import com.flink.common.deserialize.*;
import com.flink.common.kafka.KafkaManager;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.functions.TemporalTableFunction;
import org.apache.flink.types.Row;

import java.io.IOException;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class FlinkSourceBuilder extends FlinkStreamEnvAndSource {
//    public static StreamExecutionEnvironment streamEnv = null;
//    public static StreamTableEnvironment tableEnv = null;
//    public static KeyedStream<KafkaManager.KafkaTopicOffsetTimeMsg, String> d2 = null;
//    public static KeyedStream<KafkaManager.KafkaTopicOffsetTimeMsg, String> d1 = null;
//    public static SingleOutputStreamOperator<KafkaManager.KafkaTopicOffsetTimeMsg> cd2 = null;
//    public static SingleOutputStreamOperator<KafkaManager.KafkaTopicOffsetTimeMsg> cd1 = null;
//    public static  DataStreamSource<KafkaManager.KafkaTopicOffsetMsg> baseKafkaSource;
//    public static  DataStreamSource<KafkaManager.KafkaTopicOffsetTimeMsg> baseEventtimeKafkaSource;
//    public static  DataStreamSource<KafkaManager.KafkaTopicOffsetTimeMsg> baseEventtimeJsonSource;
//

    public static void init() throws IOException {
        FlinkLearnPropertiesUtil.init(EnvironmentalKey.LOCAL_PROPERTIES_PATH(),
                "WordCountEntry");
        streamEnv = FlinkEvnBuilder.buildStreamingEnv(FlinkLearnPropertiesUtil.param(),
                FlinkLearnPropertiesUtil.CHECKPOINT_PATH(),
                FlinkLearnPropertiesUtil.CHECKPOINT_INTERVAL());
        tableEnv = FlinkEvnBuilder.buildStreamTableEnv(
                streamEnv,
                Time.minutes(1),
                Time.minutes(6));
        baseKafkaSource = getKafkaDataStream("test", "localhost:9092", "latest");
        baseEventtimeKafkaSource = getKafkaDataStreamWithEventTime("test", "localhost:9092", "latest");
        baseEventtimeJsonSource = getKafkaDataStreamWithJsonEventTime("test", "localhost:9092", "latest");
        baseEventtimeJsonUidMsgSource = getKafkaDataStreamWithJsonUidMsg("test", "localhost:9092", "latest");
        baseReqImpClickSource = getKafkaDataStreamReqImpclickMsg("test", "localhost:9092", "latest" ,
                new TopicOffsetJsonReqImpClickDeserialize()
                );
    }

    public static FlinkKafkaConsumer<KafkaManager.KafkaTopicOffsetTimeMsg> getKafkaSourceWithTS(
            String topic,
            String broker,
            String reset) {
        FlinkKafkaConsumer<KafkaManager.KafkaTopicOffsetTimeMsg> kafkasource = KafkaManager.getKafkaSource(
                topic,
                broker,
                new TopicOffsetTimeStampMsgDeserialize());
        kafkasource.setCommitOffsetsOnCheckpoints(true);
        if (reset == "earliest") {
            kafkasource.setStartFromEarliest(); //不加这个默认是从上次消费
        } else if (reset == "latest") {
            kafkasource.setStartFromLatest();
        }
        return kafkasource;
    }


    public static Table getStreamTable(DataStreamSource source, String fields) {
        return tableEnv.fromDataStream(source, fields);
    }

    public static Table getStreamTable(DataStreamSource source, Expression... fields) {
        return tableEnv.fromDataStream(source, fields);
    }

    public static Table getStreamTable(SingleOutputStreamOperator source, Expression... fields) {
        return tableEnv.fromDataStream(source, fields);
    }

    public static Table getStreamTable(SingleOutputStreamOperator source, String fields) {
        return tableEnv.fromDataStream(source, fields);
    }


    /**
     * @param topic
     * @param broker
     * @param reset
     * @return
     */
    public static DataStreamSource getKafkaDataStream(String topic,
                                                      String broker,
                                                      String reset) {
        return streamEnv.addSource(getKafkaSource(topic, broker, reset));
    }

    public static FlinkKafkaConsumer<KafkaManager.KafkaTopicOffsetMsg> getKafkaSource(
            String topic,
            String broker,
            String reset) {
        FlinkKafkaConsumer<KafkaManager.KafkaTopicOffsetMsg> kafkasource = KafkaManager.getKafkaSource(
                topic,
                broker,
                new TopicOffsetMsgDeserialize());
        kafkasource.setCommitOffsetsOnCheckpoints(true);
        if (reset == "earliest") {
            kafkasource.setStartFromEarliest(); //不加这个默认是从上次消费
        } else if (reset == "latest") {
            kafkasource.setStartFromLatest();
        }
        return kafkasource;
    }

    /**
     * @param topic
     * @param broker
     * @param reset
     * @return
     */
    public static DataStreamSource getKafkaDataStreamWithEventTime(String topic,
                                                                   String broker,
                                                                   String reset) {
        return streamEnv.addSource(getKafkaSourceWithEventtime(topic, broker, reset));
    }

    /**
     * @param topic
     * @param broker
     * @param reset
     * @return
     */
    public static FlinkKafkaConsumer<KafkaManager.KafkaTopicOffsetTimeMsg> getKafkaSourceWithEventtime(
            String topic,
            String broker,
            String reset) {
        FlinkKafkaConsumer<KafkaManager.KafkaTopicOffsetTimeMsg> kafkasource = KafkaManager.getKafkaSource(
                topic,
                broker,
                new TopicOffsetTimeStampMsgDeserialize());
        kafkasource.setCommitOffsetsOnCheckpoints(true);
        if (reset == "earliest") {
            kafkasource.setStartFromEarliest(); //不加这个默认是从上次消费
        } else if (reset == "latest") {
            kafkasource.setStartFromLatest();
        }
        return kafkasource;
    }


    /**
     * @param topic
     * @param broker
     * @param reset
     * @return
     */
    public static DataStreamSource getKafkaDataStreamWithJsonEventTime(String topic,
                                                                       String broker,
                                                                       String reset) {
        return streamEnv.addSource(getKafkaSourceWithJsonEventtime(topic, broker, reset));
    }
    /**
     * @param topic
     * @param broker
     * @param reset
     * @return
     */
    public static DataStreamSource getKafkaDataStreamWithJsonUidMsg(String topic,
                                                                       String broker,
                                                                       String reset) {
        return streamEnv.addSource(getKafkaSourceWithJsonUidMsg(topic, broker, reset));
    }

    /**
     * @param topic
     * @param broker
     * @param reset
     * @return
     */
    public static DataStreamSource getKafkaDataStreamReqImpclickMsg(String topic,
                                                                    String broker,
                                                                    String reset,
                                                                    KafkaDeserializationSchema kz) {
        return streamEnv.addSource(getKafkaSourceWithJsonUidMsg(topic, broker, reset, kz));
    }
    /**
     * @param topic
     * @param broker
     * @param reset
     * @return
     */
    public static FlinkKafkaConsumer<KafkaManager.KafkaTopicOffsetTimeMsg> getKafkaSourceWithJsonEventtime(
            String topic,
            String broker,
            String reset) {
        FlinkKafkaConsumer<KafkaManager.KafkaTopicOffsetTimeMsg> kafkasource = KafkaManager.getKafkaSource(
                topic,
                broker,
                new TopicOffsetJsonEventtimeDeserialize());
        kafkasource.setCommitOffsetsOnCheckpoints(true);
        if (reset == "earliest") {
            kafkasource.setStartFromEarliest(); //不加这个默认是从上次消费
        } else if (reset == "latest") {
            kafkasource.setStartFromLatest();
        }
        return kafkasource;
    }

    /**
     * @param topic
     * @param broker
     * @param reset
     * @return
     */
    public static FlinkKafkaConsumer<KafkaManager.KafkaTopicOffsetTimeUidMsg> getKafkaSourceWithJsonUidMsg(
            String topic,
            String broker,
            String reset) {
        FlinkKafkaConsumer<KafkaManager.KafkaTopicOffsetTimeUidMsg> kafkasource = KafkaManager.getKafkaSource(
                topic,
                broker,
                new TopicOffsetJsonKeyMsgDeserialize());
        kafkasource.setCommitOffsetsOnCheckpoints(true);
        if (reset == "earliest") {
            kafkasource.setStartFromEarliest(); //不加这个默认是从上次消费
        } else if (reset == "latest") {
            kafkasource.setStartFromLatest();
        }
        return kafkasource;
    }
    /**
     * @param topic
     * @param broker
     * @param reset
     * @return
     */
    public static FlinkKafkaConsumer getKafkaSourceWithJsonUidMsg(
            String topic,
            String broker,
            String reset,
            KafkaDeserializationSchema kz
    ) {
        FlinkKafkaConsumer kafkasource = KafkaManager.getKafkaSource(
                topic,
                broker,
                kz);
        kafkasource.setCommitOffsetsOnCheckpoints(true);
        if (reset == "earliest") {
            kafkasource.setStartFromEarliest(); //不加这个默认是从上次消费
        } else if (reset == "latest") {
            kafkasource.setStartFromLatest();
        }
        return kafkasource;
    }
    /**
     * json的数据源
     */
    public static void initJsonCleanSource() {
        cd1 = baseEventtimeJsonSource
                .assignTimestampsAndWatermarks(
                        new BoundedOutOfOrdernessTimestampExtractor
                                <KafkaManager.KafkaTopicOffsetTimeMsg>(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)) {
                            @Override
                            public long extractTimestamp(KafkaManager.KafkaTopicOffsetTimeMsg element) {
                                return element.ts();
                            }
                        }
                );
        cd2 = getKafkaDataStreamWithJsonEventTime("test2", "localhost:9092", "latest")
                .assignTimestampsAndWatermarks(
                        new BoundedOutOfOrdernessTimestampExtractor
                                <KafkaManager.KafkaTopicOffsetTimeMsg>(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)) {
                            @Override
                            public long extractTimestamp(KafkaManager.KafkaTopicOffsetTimeMsg element) {
                                return element.ts();
                            }
                        }
                )
        ;

        cd3 = getKafkaDataStreamWithJsonEventTime("test3", "localhost:9092", "latest")
                .assignTimestampsAndWatermarks(
                        new BoundedOutOfOrdernessTimestampExtractor
                                <KafkaManager.KafkaTopicOffsetTimeMsg>(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)) {
                            @Override
                            public long extractTimestamp(KafkaManager.KafkaTopicOffsetTimeMsg element) {
                                return element.ts();
                            }
                        }
                )
        ;
    }


    /**
     * json的数据源
     */
    public static void initJsonUidMsgSource() {
        uid1T = getKafkaDataStreamWithJsonUidMsg("test", "localhost:9092", "latest")
                .assignTimestampsAndWatermarks(
                        new BoundedOutOfOrdernessTimestampExtractor
                                <KafkaManager.KafkaTopicOffsetTimeUidMsg>(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)) {
                            @Override
                            public long extractTimestamp(KafkaManager.KafkaTopicOffsetTimeUidMsg element) {
                                return element.ts();
                            }
                        }
                );
        uid2T = getKafkaDataStreamWithJsonUidMsg("test2", "localhost:9092", "latest")
                .assignTimestampsAndWatermarks(
                        new BoundedOutOfOrdernessTimestampExtractor
                                <KafkaManager.KafkaTopicOffsetTimeUidMsg>(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)) {
                            @Override
                            public long extractTimestamp(KafkaManager.KafkaTopicOffsetTimeUidMsg element) {
                                return element.ts();
                            }
                        }
                )
        ;

        uid3T = getKafkaDataStreamWithJsonUidMsg("test3", "localhost:9092", "latest")
                .assignTimestampsAndWatermarks(
                        new BoundedOutOfOrdernessTimestampExtractor
                                <KafkaManager.KafkaTopicOffsetTimeUidMsg>(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)) {
                            @Override
                            public long extractTimestamp(KafkaManager.KafkaTopicOffsetTimeUidMsg element) {
                                return element.ts();
                            }
                        }
                )
        ;
    }


    public static void initSource() {
        d1 = baseEventtimeKafkaSource
                .assignTimestampsAndWatermarks(
                        new BoundedOutOfOrdernessTimestampExtractor
                                <KafkaManager.KafkaTopicOffsetTimeMsg>(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)) {
                            @Override
                            public long extractTimestamp(KafkaManager.KafkaTopicOffsetTimeMsg element) {
                                return element.ts();
                            }
                        }
                ).keyBy(KafkaManager.KafkaTopicOffsetTimeMsg::msg);
        d2 = getKafkaDataStreamWithEventTime("test2", "localhost:9092", "latest")
                .assignTimestampsAndWatermarks(
                        new BoundedOutOfOrdernessTimestampExtractor
                                <KafkaManager.KafkaTopicOffsetTimeMsg>(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)) {
                            @Override
                            public long extractTimestamp(KafkaManager.KafkaTopicOffsetTimeMsg element) {
                                return element.ts();
                            }
                        }
                )
                .keyBy((KeySelector<KafkaManager.KafkaTopicOffsetTimeMsg, String>) value -> value.msg());
    }

    /**
     * json的数据源
     */
    public static void initJsonSource(Boolean needWtm) {
        if(needWtm){
            d1 = baseEventtimeJsonSource
                    .assignTimestampsAndWatermarks(
                            new BoundedOutOfOrdernessTimestampExtractor
                                    <KafkaManager.KafkaTopicOffsetTimeMsg>(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)) {
                                @Override
                                public long extractTimestamp(KafkaManager.KafkaTopicOffsetTimeMsg element) {
                                    return element.ts();
                                }
                            }
                    )
                    .keyBy(KafkaManager.KafkaTopicOffsetTimeMsg::msg);
        } else {
            d1 = baseEventtimeJsonSource
                    .keyBy(KafkaManager.KafkaTopicOffsetTimeMsg::msg);
        }
        if(needWtm){
            d2 = getKafkaDataStreamWithJsonEventTime("test2", "localhost:9092", "latest")
                    .assignTimestampsAndWatermarks(
                            new BoundedOutOfOrdernessTimestampExtractor
                                    <KafkaManager.KafkaTopicOffsetTimeMsg>(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)) {
                                @Override
                                public long extractTimestamp(KafkaManager.KafkaTopicOffsetTimeMsg element) {
                                    return element.ts();
                                }
                            }
                    )
                    .keyBy((KeySelector<KafkaManager.KafkaTopicOffsetTimeMsg, String>) value -> value.msg());
        } else {
            d2 = getKafkaDataStreamWithJsonEventTime("test2", "localhost:9092", "latest")
                    .keyBy((KeySelector<KafkaManager.KafkaTopicOffsetTimeMsg, String>) value -> value.msg());
        }


    }

    public static void printlnStringTable(Table b) {
        tableEnv.toRetractStream(b,
                Row.class)
                .print();
    }
}
