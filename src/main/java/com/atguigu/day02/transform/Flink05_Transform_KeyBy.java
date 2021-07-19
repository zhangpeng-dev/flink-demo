package com.atguigu.day02.transform;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName Flink05_Transform_KeyBy
 * @Description TODO
 * @Author ASUS
 * @Date 2021/7/13 19:30
 * @Version 1.0
 **/
public class Flink05_Transform_KeyBy {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);

        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<WaterSensor> flatMap = streamSource.flatMap((FlatMapFunction<String, WaterSensor>) (line, out) -> {
            String[] strings = line.split(",");
            out.collect(new WaterSensor(strings[0], Long.parseLong(strings[1]), Integer.parseInt(strings[2])));
        }).returns(WaterSensor.class);

        flatMap.keyBy("id").print();

        env.execute();
    }
}