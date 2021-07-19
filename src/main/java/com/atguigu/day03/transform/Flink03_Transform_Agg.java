package com.atguigu.day03.transform;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName Flink03_Transform_agg
 * @Description TODO
 * @Author ASUS
 * @Date 2021/7/14 11:27
 * @Version 1.0
 **/
public class Flink03_Transform_Agg {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<WaterSensor> flatMap = streamSource.flatMap((FlatMapFunction<String, WaterSensor>) (line, out) -> {
            String[] split = line.split(",");
            out.collect(new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2])));
        }).returns(WaterSensor.class);

        KeyedStream<WaterSensor, Tuple> keyedStream = flatMap.keyBy("id");

        keyedStream.max("vc").print();

        keyedStream.maxBy("vc",true).print();

        keyedStream.maxBy("vc",false).print();

        env.execute();
    }
}
