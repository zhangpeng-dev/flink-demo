package com.atguigu.day06;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @ClassName Flink07_BroadcastState
 * @Description TODO
 * @Author ASUS
 * @Date 2021/7/19 19:07
 * @Version 1.0
 **/
public class Flink07_BroadcastState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(3);

        DataStreamSource<String> source1 = env.socketTextStream("hadoop102", 9999);
        DataStreamSource<String> source2 = env.socketTextStream("hadoop102", 8888);

        MapStateDescriptor<String, String> descriptor = new MapStateDescriptor<>(
                "broadcast", String.class, String.class);
        BroadcastStream<String> broadcast = source2.broadcast(descriptor);

        source1.connect(broadcast)
                .process(new BroadcastProcessFunction<String, String, String>() {
                    @Override
                    public void processElement(String value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                        ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(descriptor);
                        if ("1".equals(broadcastState.get("switch"))) {
                            out.collect("切换到1号配置");
                        } else if ("2".equals(broadcastState.get("switch"))) {
                            out.collect("切换到2号配置");
                        } else {
                            out.collect("切换到其他配置");
                        }
                    }

                    @Override
                    public void processBroadcastElement(String value, Context ctx, Collector<String> out) throws Exception {
                        BroadcastState<String, String> broadcastState = ctx.getBroadcastState(descriptor);
                        broadcastState.put("switch", value);
                    }
                }).print();

        env.execute();
    }
}
