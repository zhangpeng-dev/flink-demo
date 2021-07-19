package com.atguigu.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName Flink03_WordCount_Unbounded
 * @Description TODO
 * @Author ASUS
 * @Date 2021/7/12 11:31
 * @Version 1.0
 **/
public class Flink03_WordCount_Unbounded {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();

        senv.setParallelism(1);

        DataStreamSource<String> streamSource = senv.socketTextStream("hadoop102", 9999);

        streamSource
                .flatMap((FlatMapFunction<String, Tuple2<String, Long>>) (value, out) -> {
                    for (String word : value.split(" ")) {
                        out.collect(Tuple2.of(word, 1L));
                    }
                })
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(tuple -> tuple.f0)
                .sum(1)
                .print();

        streamSource
                .flatMap((FlatMapFunction<String, String>) (value, out) -> {
                    for (String word : value.split(" ")) {
                        out.collect(word);
                    }
                })
                .returns(Types.STRING)
                .map(word -> Tuple2.of(word, 1L))
                .returns(Types.TUPLE(Types.STRING,Types.LONG))
                .keyBy(tuple -> tuple.f0)
                .sum(1)
                .print();
        senv.execute();
    }
}