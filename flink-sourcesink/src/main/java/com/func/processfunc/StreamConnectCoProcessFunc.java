package com.func.processfunc;

import com.pojo.KafkaMessgePoJo;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.IOException;

public class StreamConnectCoProcessFunc extends KeyedCoProcessFunction<Object, KafkaMessgePoJo, KafkaMessgePoJo, Tuple3<Boolean, KafkaMessgePoJo, KafkaMessgePoJo>> {
    // 一对多的场景，v2作为维表
    ListState<KafkaMessgePoJo> source = null;
    ValueState<KafkaMessgePoJo> v2 = null;
    OutputTag<String> errV1 = null;

    public StreamConnectCoProcessFunc(OutputTag<String> errV1) {
        this.errV1 = errV1;
    }

    /**
     * 初始化
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        source = getRuntimeContext().getListState(
                new ListStateDescriptor("test1",
                        TypeInformation.of(KafkaMessgePoJo.class)));
        v2 = getRuntimeContext().getState(
                new ValueStateDescriptor("test2",
                        TypeInformation.of(KafkaMessgePoJo.class)));
    }

    /**
     * 这里面的watermark是两个共同决定的。
     * register time server 触发时执行: 清除所有state
     * @param timestamp
     * @param ctx
     * @param out
     * @throws Exception
     */
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple3<Boolean, KafkaMessgePoJo, KafkaMessgePoJo>> out) throws Exception {
//        System.out.println("#### Ontime ####");
        if (v2.value() != null) {
            source.get().forEach(x -> {
                try {
                    out.collect(new Tuple3<>(true, x, v2.value()));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        } else {
            source.get().forEach(x -> {
                    out.collect(new Tuple3<>(false, x, null));
//                if(x.ts() <= timestamp){
//                    if(errV1 != null)
//                    ctx.output(errV1, "过期未Join : " + x);
//                }
            });
        }
        source.clear();
        v2.clear();
    }

    /**
     *
     * @param value
     * @param ctx
     * @param out
     * @throws Exception
     */
    @Override
    public void processElement1(KafkaMessgePoJo value, Context ctx, Collector<Tuple3<Boolean, KafkaMessgePoJo, KafkaMessgePoJo>> out) throws Exception {
//        System.out.println(value+ "  source wtm : " + ctx.timerService().currentWatermark());
        if (v2.value() == null) {
            source.add(value);
//            System.out.println("未Join到等待 : " + ctx.getCurrentKey());
            // 如果维表的更新频率很慢，可能需要使用processtime。不然无法触发
//            ctx.timerService().registerEventTimeTimer(value.ts()); // 当wartermark超过这个时间的时候就触发ontimer
            ctx.timerService().registerProcessingTimeTimer(System.currentTimeMillis() + 60000L); // 1分钟后触发
        } else {
            out.collect(new Tuple3<>(true, value, v2.value()));
            source.get().forEach(x -> {
                try {
                    out.collect(new Tuple3<>(true, value, v2.value()));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            source.clear();
        }
    }

    /**
     * 维表数据做记录
     * @param value
     * @param ctx
     * @param out
     * @throws Exception
     */
    @Override
    public void processElement2(KafkaMessgePoJo value, Context ctx, Collector<Tuple3<Boolean, KafkaMessgePoJo, KafkaMessgePoJo>> out) throws Exception {
//        System.out.println(value +  "  v2 wtm : " + ctx.timerService().currentWatermark());
        v2.update(value);
        source.get().forEach(x -> {
            try {
                out.collect(new Tuple3(true, x, v2.value()));
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        source.clear();
    }
}
