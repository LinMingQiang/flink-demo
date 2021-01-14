package com.flink.learn.entry;

import com.flink.common.core.EnvironmentalKey;
import com.flink.common.core.FlinkLearnPropertiesUtil;
import com.flink.common.deserialize.TopicOffsetMsgDeserialize;
import com.flink.common.java.core.FlinkEvnBuilder;
import com.flink.common.java.core.FlinkSourceBuilder;
import com.flink.common.java.core.FlinkStreamEnvAndSource;
import com.flink.common.java.manager.KafkaSourceManager;
import com.flink.common.kafka.KafkaManager;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;
import java.util.Map;

public class CepDemoEntry extends FlinkStreamEnvAndSource {

    public static void main(String[] args) throws Exception {
        FlinkLearnPropertiesUtil.init(EnvironmentalKey.LOCAL_PROPERTIES_PATH(),
                "CepDemoEntry");
        streamEnv = FlinkEvnBuilder.buildStreamingEnv(FlinkLearnPropertiesUtil.param(),
                FlinkLearnPropertiesUtil.CHECKPOINT_PATH(),
                FlinkLearnPropertiesUtil.CHECKPOINT_INTERVAL());

        DataStreamSource<KafkaManager.KafkaTopicOffsetTimeMsg> d1 =
                FlinkSourceBuilder.getKafkaDataStreamWithJsonEventTime("test", "localhost:9092", "latest");
        Pattern<KafkaManager.KafkaTopicOffsetTimeMsg, KafkaManager.KafkaTopicOffsetTimeMsg> req_imp =
                Pattern.<KafkaManager.KafkaTopicOffsetTimeMsg>begin("start")
                        .where(new SimpleCondition<KafkaManager.KafkaTopicOffsetTimeMsg>() {
                            @Override
                            public boolean filter(KafkaManager.KafkaTopicOffsetTimeMsg kafkaTopicOffsetTimeMsg) throws Exception {
                                return kafkaTopicOffsetTimeMsg.msg().equals("start");
                            }
                        })
                        .next("middle") // 接着有imp
                        .where(new SimpleCondition<KafkaManager.KafkaTopicOffsetTimeMsg>() {
                            @Override
                            public boolean filter(KafkaManager.KafkaTopicOffsetTimeMsg kafkaTopicOffsetTimeMsg) throws Exception {
                                System.out.println("middle");
                                return kafkaTopicOffsetTimeMsg.msg().equals("middle");
                            }
                        })
//                        .followedByAny("end") // 接着有imp
//                        .where(new SimpleCondition<KafkaTopicOffsetTimeMsg>() {
//                            @Override
//                            public boolean filter(KafkaTopicOffsetTimeMsg kafkaTopicOffsetTimeMsg) throws Exception {
//                                System.out.println("end");
//                                return kafkaTopicOffsetTimeMsg.msg().equals("end");
//                            }
//                        })
                ;

        CEP
                .pattern(d1, req_imp)
                .inProcessingTime()
                .select(new PatternSelectFunction<KafkaManager.KafkaTopicOffsetTimeMsg, String>() {
                    @Override
                    public String select(Map<String, List<KafkaManager.KafkaTopicOffsetTimeMsg>> map) throws Exception {
                        return map.toString();
                    }
                })
                .print();

        streamEnv.execute();
    }
}
