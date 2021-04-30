package com.flink.java.test;

import com.flink.common.kafka.KafkaManager.*;
import com.flink.learn.test.common.FlinkJavaStreamTableTestBase;
import com.pojo.KafkaMessgePoJo;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.junit.Test;
public class FlinkStreamDAUTest extends FlinkJavaStreamTableTestBase {

    /**
     * DAU ： 一天的窗口，每5s输出一次。
     * 不能用滑动窗口， 用翻滚窗口+trigger （翻滚窗口肯定是一天的 0-24，不会有滑动的问题）
     * 当我们需要计算两个报表的时候。
     * 1: day : pv, uv
     * 2: day + msg ： pv，uv
     * 方法1： 开始的时候将一条数据变两条，然后keyby的时候统一用key (但是这样uid也会翻)
     * 方法2： 先按 day+ msg去重，然后再 day去重。。。如果报表多了，有点麻烦
     */
    @Test
    public void testWindowDAU() throws Exception {
        // {"ts":5,"uid":"u1","msg":"c1"} {"ts":10,"uid":"u1","msg":"c1"} {"ts":15,"uid":"u1","msg":"c1"}
        // {"ts":20,"uid":"u1","msg":"c2"} {"ts":199,"uid":"u1","msg":"c2"} {"ts":100,"uid":"u1","msg":"c2"}
//        initJsonUidMsgSource();
        getKafkaDataStreamSource("test", "localhost:9092", "latest")
                // .map() // 方法1： 开始的时候将一条数据变两条，然后keyby的时候统一用key
                .keyBy((KeySelector<KafkaMessgePoJo, String>) value -> value.msg) // 按key分配
                .window(TumblingEventTimeWindows.of(Time.seconds(100L))) // 统计100s一个窗口
                // 因为用的是系统时间，所以一个窗口会被多次触发，除非wtm超过了这个窗口的endtime，否则窗口一直保留.用processtime可以多次触发
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(3))) // 固定时间触发, 每5s触发一次(系统时间)
                .evictor(TimeEvictor.of(Time.seconds(0), true)) // 要定时清理窗口数据，否则会一直触发，即使没有数据
                // 如果不加这个， Iterable<KafkaTopicOffsetTimeUidMsg> elements 的数据就一直累积。
                // 每次计算完都清除 窗口数据。(只是清理原始数据process的数据会保留，也就是说，每次计算的时候，都是计算都是拿着5s中的数据进入process计算)
                .process(new ProcessWindowFunction<KafkaMessgePoJo,
                        Tuple2<TimeWindow, Tuple3<String, Long, Long>>,
                        String,
                        TimeWindow>() {
                    ValueStateDescriptor<Long> pvDec = null;
                    MapStateDescriptor<String, String> uidDec = null;
                    ValueStateDescriptor<Long> uvDec = null;

                    @Override
                    public void open(Configuration parameters) {
                        uidDec = new MapStateDescriptor("uidState", Types.STRING, Types.STRING);
                        pvDec = new ValueStateDescriptor<Long>("pvCount", Types.LONG, 0L);
                        uvDec = new ValueStateDescriptor<Long>("uvCount", Types.LONG, 0L);
                    }
                    // 不同的窗口也会进来，所以必须用 context.windowState() ,这个是窗口自己的state。如果在外面定义，那就是operate state，所有公用的
                    // 触发的时候会拿到当前window的context，这里面包含了window的state： context是ProcessContext
                    @Override
                    public void process(String groupKey,
                                        Context context,
                                        Iterable<KafkaMessgePoJo> elements,
                                        Collector<Tuple2<TimeWindow, Tuple3<String, Long, Long>>> out) throws Exception {
                        ValueState<Long> pvState = context.windowState().getState(pvDec);
                        ValueState<Long> uvState = context.windowState().getState(uvDec);
                        MapState<String, String> uidsState = context.windowState().getMapState(uidDec);
                        Long pv = pvState.value();
                        Long uv = uvState.value();
                        for (KafkaMessgePoJo element : elements) {
                            if (!uidsState.contains(element.uid)) {
                                uidsState.put(element.uid, null);
                                uv++;
                            }
                            pv++;
                        }
                        pvState.update(pv);
                        uvState.update(uv);
                        out.collect(new Tuple2(context.window(), new Tuple3(groupKey, pv, uvState.value())));
                    }

                    /**
                     * 当窗口被清理的时候调用对应窗口的clear
                     * @param context
                     * @throws Exception
                     */
                    @Override
                    public void clear(Context context) throws Exception {
                        System.out.println("clear : " + context.window());
                        ValueState<Long> pvState = context.windowState().getState(pvDec);
                        ValueState<Long> uvState = context.windowState().getState(uvDec);
                        MapState<String, String> uidsState = context.windowState().getMapState(uidDec);
                        pvState.clear();
                        uvState.clear();
                        uidsState.clear();
                        super.clear(context);
                    }
                })
                .print();
        streamEnv.execute("lmq-flink-demo"); //程序名
    }
}
