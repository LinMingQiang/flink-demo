package com.core;

import com.flink.common.kafka.KafkaManager;
import com.pojo.KafkaMessgePoJo;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public abstract class FlinkStreamEnvAndSource {
    public static ExecutionEnvironment bEnv = null;
    public static StreamExecutionEnvironment streamEnv = null;
    public static StreamTableEnvironment tableEnv = null;

    public static KeyedStream<KafkaMessgePoJo, String> kafkaDataSource = null;
    public static Table kafkaDataTable = null;

}
