package com.atguigu.day04.windows;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;

/**
 * @ClassName Flink08_Window_Function_Aggregate
 * @Description TODO
 * @Author ASUS
 * @Date 2021/7/16 19:33
 * @Version 1.0
 **/
public class Flink08_Window_Function_Aggregate {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        source.map(line -> {
            String[] split = line.split(",");
            return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
        }).returns(WaterSensor.class)
                .keyBy(WaterSensor::getId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .aggregate(new AggregateFunction<WaterSensor, Long, Long>() {

                    @Override
                    public Long createAccumulator() {
                        System.out.println("createAccumulator");
                        return 0L;
                    }

                    @Override
                    public Long add(WaterSensor value, Long accumulator) {
                        System.out.println("add");
                        return value.getVc()+accumulator;
                    }

                    @Override
                    public Long getResult(Long accumulator) {
                        System.out.println("getResult");
                        return accumulator;
                    }

                    @Override
                    public Long merge(Long a, Long b) {
                        System.out.println("merge");
                        return a+b;
                    }
                })
                .print();

        env.execute();
    }
}
