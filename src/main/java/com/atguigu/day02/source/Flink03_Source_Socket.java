package com.atguigu.day02.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName Flink03_Source_Socket
 * @Description TODO
 * @Author ASUS
 * @Date 2021/7/13 16:34
 * @Version 1.0
 **/
public class Flink03_Source_Socket {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        streamSource.print();

        env.execute();
    }
}
