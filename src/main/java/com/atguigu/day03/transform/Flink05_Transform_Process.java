package com.atguigu.day03.transform;

import com.alibaba.fastjson.JSON;
import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @ClassName Flink05_Transform_Process
 * @Description TODO
 * @Author ASUS
 * @Date 2021/7/14 11:40
 * @Version 1.0
 **/
public class Flink05_Transform_Process {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        streamSource.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                String[] split = value.split(",");
                WaterSensor waterSensor = new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                out.collect(JSON.toJSONString(waterSensor));
            }
        }).print();

        SingleOutputStreamOperator<WaterSensor> waterSensor = streamSource.process(new ProcessFunction<String, WaterSensor>() {
            @Override
            public void processElement(String value, Context ctx, Collector<WaterSensor> out) throws Exception {
                String[] split = value.split(",");
                WaterSensor waterSensor = new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                out.collect(waterSensor);
            }
        });

        KeyedStream<WaterSensor, String> keyedStream = waterSensor.keyBy(WaterSensor::getId);

        keyedStream.process(new KeyedProcessFunction<String, WaterSensor, String>() {
            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                out.collect(JSON.toJSONString(value));
            }
        }).print();

        env.execute();
    }
}
