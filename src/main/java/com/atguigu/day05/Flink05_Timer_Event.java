package com.atguigu.day05;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;

/**
 * @ClassName Flink05_Timer_Event
 * @Description TODO
 * @Author ASUS
 * @Date 2021/7/18 22:18
 * @Version 1.0
 **/
public class Flink05_Timer_Event {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<WaterSensor> map = source.map(line -> {
            String[] split = line.split(",");
            return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
        }).returns(WaterSensor.class);

        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
        map
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner((SerializableTimestampAssigner<WaterSensor>) (e, r) -> e.getTs() * 1000))
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                        ctx.timerService().registerEventTimeTimer(ctx.timestamp()+5000);
                        out.collect(ctx.timestamp()/1000+"定时闹钟");
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect(timestamp/1000+"闹钟响了");
                    }
                }).print();

        env.execute();
    }
}
