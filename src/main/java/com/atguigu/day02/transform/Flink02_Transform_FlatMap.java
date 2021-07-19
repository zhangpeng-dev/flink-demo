package com.atguigu.day02.transform;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @ClassName Flink02_Transform_FlatMap
 * @Description TODO
 * @Author ASUS
 * @Date 2021/7/13 18:50
 * @Version 1.0
 **/
public class Flink02_Transform_FlatMap {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        streamSource.flatMap((FlatMapFunction<String, String>) (value, out) -> {
            for (String word : value.split(" ")) {
                out.collect(word);
            }
        }).returns(Types.STRING).print();

        env.execute();
    }
}