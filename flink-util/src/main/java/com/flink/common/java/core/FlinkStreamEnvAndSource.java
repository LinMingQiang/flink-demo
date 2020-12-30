package com.flink.common.java.core;

import com.flink.common.kafka.KafkaManager;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public abstract class FlinkStreamEnvAndSource {
    public static StreamExecutionEnvironment streamEnv = null;
    public static StreamTableEnvironment tableEnv = null;
    public static KeyedStream<KafkaManager.KafkaTopicOffsetTimeMsg, String> d2 = null;
    public static KeyedStream<KafkaManager.KafkaTopicOffsetTimeMsg, String> d1 = null;
    public static SingleOutputStreamOperator<KafkaManager.KafkaTopicOffsetTimeMsg> cd2 = null;
    public static SingleOutputStreamOperator<KafkaManager.KafkaTopicOffsetTimeMsg> cd1 = null;
    public static SingleOutputStreamOperator<KafkaManager.KafkaTopicOffsetTimeMsg> cd3 = null;

    public static SingleOutputStreamOperator<KafkaManager.KafkaTopicOffsetTimeUidMsg> uid1T = null;
    public static SingleOutputStreamOperator<KafkaManager.KafkaTopicOffsetTimeUidMsg> uid2T = null;
    public static SingleOutputStreamOperator<KafkaManager.KafkaTopicOffsetTimeUidMsg> uid3T = null;


    public static DataStreamSource<KafkaManager.KafkaTopicOffsetMsg> baseKafkaSource = null;
    public static DataStreamSource<KafkaManager.KafkaTopicOffsetTimeMsg> baseEventtimeKafkaSource = null;
    public static DataStreamSource<KafkaManager.KafkaTopicOffsetTimeMsg> baseEventtimeJsonSource = null;
    public static DataStreamSource<KafkaManager.KafkaTopicOffsetTimeUidMsg> baseEventtimeJsonUidMsgSource = null;


    public static TableEnvironment tableE = null;
    public static ExecutionEnvironment bEnv = null;
}
