package com.atguigu.day03.transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName Flink02_Transform_Union
 * @Description TODO
 * @Author ASUS
 * @Date 2021/7/14 11:24
 * @Version 1.0
 **/
public class Flink02_Transform_Union {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> source1 = env.fromElements("1", "2", "3", "4", "5", "6");

        DataStreamSource<String> source2 = env.fromElements("a", "b", "c", "d", "e", "f");

        DataStream<String> union = source1.union(source2);

        union.print();

        env.execute();
    }
}
