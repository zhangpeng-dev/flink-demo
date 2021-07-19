package com.atguigu.day03.test;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName Test02
 * @Description TODO
 * @Author ASUS
 * @Date 2021/7/15 10:00
 * @Version 1.0
 **/
public class Test02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> source = env.readTextFile("input/word.txt");

        SingleOutputStreamOperator<Tuple2<String, Long>> returns = source
                .flatMap((FlatMapFunction<String, Tuple2<String, Long>>) (value, out) -> {
            String[] words = value.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));

        KeyedStream<Tuple2<String, Long>, String> keyedStream = returns.keyBy(t -> t.f0);

        keyedStream.print();
        keyedStream.sum(1).print();

        env.execute();
    }
}
