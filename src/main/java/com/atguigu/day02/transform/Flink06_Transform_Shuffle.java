package com.atguigu.day02.transform;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName Flink06_Transform_Shuffle
 * @Description TODO
 * @Author ASUS
 * @Date 2021/7/13 20:19
 * @Version 1.0
 **/
public class Flink06_Transform_Shuffle {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        DataStream<String> shuffle = streamSource.shuffle();

        shuffle.print().setParallelism(3);

        env.execute();
    }
}