package com.atguigu.day02.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName Flink02_Source_File
 * @Description TODO
 * @Author ASUS
 * @Date 2021/7/13 16:32
 * @Version 1.0
 **/
public class Flink02_Source_File {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.readTextFile("input/word.txt");

        streamSource.print();

        env.execute();
    }
}
