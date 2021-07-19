package com.atguigu.day03.transform;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName Flink04_Transform_Reduce
 * @Description TODO
 * @Author ASUS
 * @Date 2021/7/14 11:34
 * @Version 1.0
 **/
public class Flink04_Transform_Reduce {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<WaterSensor> flatMap = streamSource.flatMap((FlatMapFunction<String, WaterSensor>) (line, out) -> {
            String[] split = line.split(",");
            out.collect(new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2])));
        }).returns(WaterSensor.class);

        KeyedStream<WaterSensor, String> keyedStream = flatMap.keyBy(WaterSensor::getId);

        keyedStream.reduce((ReduceFunction<WaterSensor>) (value1, value2) -> {
            return new WaterSensor(value1.getId(), value1.getTs(), Math.max(value1.getVc(), value2.getVc()));
        }).print();

        env.execute();
    }
}
