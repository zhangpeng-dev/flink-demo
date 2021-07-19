package com.atguigu.test;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @ClassName Test01
 * @Description TODO
 * @Author ASUS
 * @Date 2021/7/18 17:44
 * @Version 1.0
 **/
public class Test01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<WaterSensor> map = source.map(line -> {
            String[] split = line.split(",");
            return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));

        }).returns(WaterSensor.class);

        SingleOutputStreamOperator<String> result = map
                .assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(
                        Duration.ofSeconds(2)).withTimestampAssigner((SerializableTimestampAssigner<WaterSensor>)
                        (element, recordTimestamp) -> element.getTs() * 1000))
                .keyBy(WaterSensor::getId)
                .window(SlidingEventTimeWindows.of(Time.seconds(6), Time.seconds(2)))
                .allowedLateness(Time.seconds(2))
                .sideOutputLateData(new OutputTag<WaterSensor>("side"){})
                .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<WaterSensor> elements,
                                        Collector<String> out) throws Exception {
                        String msg = "当前key: " + key
                                + "窗口: [" + context.window().getStart() / 1000 + ","
                                + context.window().getEnd() / 1000 + "] 一共有 "
                                + elements.spliterator().estimateSize() + "条数据 ";
                        out.collect(msg);
                    }
                });

        result.print();
        result.getSideOutput(new OutputTag<WaterSensor>("side"){}).print();


        env.execute();
    }
}
