package com.atguigu.day03.transform;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * @ClassName Flink01_Transform_Connect
 * @Description TODO
 * @Author ASUS
 * @Date 2021/7/14 11:19
 * @Version 1.0
 **/
public class Flink01_Transform_Connect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<Integer> source1 = env.fromElements(1, 2, 3, 4, 5, 6);

        DataStreamSource<String> source2 = env.fromElements("a", "b", "c", "d", "e", "f");

        ConnectedStreams<Integer, String> connect = source1.connect(source2);

        connect.map(new CoMapFunction<Integer, String, String>() {
            @Override
            public String map1(Integer value) throws Exception {
                return "source1:"+value;
            }

            @Override
            public String map2(String value) throws Exception {
                return "source2:"+value;
            }
        }).print();

        env.execute();
    }
}
