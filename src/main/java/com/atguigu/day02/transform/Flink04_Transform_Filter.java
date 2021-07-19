package com.atguigu.day02.transform;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName Flink04_Transform_Filter
 * @Description TODO
 * @Author ASUS
 * @Date 2021/7/13 19:16
 * @Version 1.0
 **/
public class Flink04_Transform_Filter {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        streamSource.filter(num->Integer.parseInt(num)%2==0).print();

        env.execute();
    }
}
