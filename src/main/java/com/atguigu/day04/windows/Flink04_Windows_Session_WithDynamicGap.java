package com.atguigu.day04.windows;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;

/**
 * @ClassName Flink04_Windows_Session_WithDynamicGap
 * @Description TODO
 * @Author ASUS
 * @Date 2021/7/16 18:46
 * @Version 1.0
 **/
public class Flink04_Windows_Session_WithDynamicGap {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        source.map(line -> {
            String[] split = line.split(",");
            return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
        }).returns(WaterSensor.class)
                .keyBy(WaterSensor::getId)
                .window(ProcessingTimeSessionWindows.withDynamicGap(WaterSensor::getTs))
                .process(new ProcessWindowFunction<WaterSensor, WaterSensor, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<WaterSensor> elements, Collector<WaterSensor> out) throws Exception {
                        System.out.println(sdf.format(context.window().getStart()));
                        for (WaterSensor element : elements) {
                            out.collect(element);
                        }
                        System.out.println(sdf.format(context.window().getEnd()));
                    }
                }).print();

        env.execute();
    }
}
