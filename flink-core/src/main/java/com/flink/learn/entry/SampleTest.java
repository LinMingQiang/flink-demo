package com.flink.learn.entry;

import com.flink.common.deserialize.KafkaMessageDeserialize;
import com.flink.common.kafka.KafkaManager;
import com.flink.learn.bean.WordCountPoJo;
import com.pojo.KafkaMessgePoJo;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.io.IOException;

public class SampleTest {
    public static StreamExecutionEnvironment env = null;
    public static String checkpointPath = "file:///Users/eminem/workspace/flink/flink-learn/checkpoint/WordCountEntry";

    public static void main(String[] args) throws Exception {
        String checkpointPath = "file:///tmp/WordCount";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        env.setParallelism(1);

        CheckpointConfig config = env.getCheckpointConfig();
        config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setStateBackend(new RocksDBStateBackend(checkpointPath, true));


        DataStream<String> baseKafkaSource = env.socketTextStream("localhost", 9877);
        baseKafkaSource.flatMap((FlatMapFunction<String, String>) (value, out) -> {
            for (String s : value.split(",", -1)) {
                out.collect(s);
            }
        })
                .returns(Types.STRING)
                .map(x -> new Tuple2(x, 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(x -> x.f0)
                .sum(1)
                .setParallelism(1)
                .print();

        env.execute("stream word count job");


//
//        streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
//        RocksDBStateBackend rocksDBStateBackend = null;
//        try {
//            rocksDBStateBackend = new RocksDBStateBackend(checkpointPath, true);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        streamEnv.setStateBackend(rocksDBStateBackend);
//        DataStreamSource<KafkaManager.KafkaTopicOffsetMsg> baseKafkaSource =  getKafkaDataStream("test", "localhost:9092", "latest");
//        baseKafkaSource.flatMap((FlatMapFunction<KafkaManager.KafkaTopicOffsetMsg, String>) (value, out) -> {
//                    for (String s : value.msg().split(",", -1)) {
//                        System.out.println(s);
//                        out.collect(s);
//                    }
//                })
//                .returns(Types.STRING)
//                .map(x -> new Tuple2(x, 1L))
//                .returns(Types.TUPLE(Types.STRING, Types.LONG))
//                .keyBy(x -> x.f0)
//                .sum(1)
//                .setParallelism(1)
//                .print();
//
//        streamEnv.execute("lmq-flink-demo"); //程序名

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
        return env.addSource(getKafkaSource(topic, broker, reset));
    }

    public static FlinkKafkaConsumer<KafkaMessgePoJo> getKafkaSource(
            String topic,
            String broker,
            String reset) {
        FlinkKafkaConsumer<KafkaMessgePoJo> kafkasource = KafkaManager.getKafkaSource(
                topic,
                broker,
                new KafkaMessageDeserialize());
        kafkasource.setCommitOffsetsOnCheckpoints(true);
        if (reset == "earliest") {
            kafkasource.setStartFromEarliest(); //不加这个默认是从上次消费
        } else if (reset == "latest") {
            kafkasource.setStartFromLatest();
        }
        return kafkasource;
    }
}
