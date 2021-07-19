package com.atguigu.day04.project;

import com.atguigu.day04.bean.UserBehavior;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName Flink01_Project_PV
 * @Description TODO
 * @Author ASUS
 * @Date 2021/7/15 10:54
 * @Version 1.0
 **/
public class Flink01_Project_PV {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> source = env.readTextFile("input/UserBehavior.csv");

//        SingleOutputStreamOperator<UserBehavior> returns = source
//                .flatMap((FlatMapFunction<String, UserBehavior>) (value, out) -> {
//                    String[] split = value.split(",");
//                    out.collect(new UserBehavior(Long.parseLong(split[0]), Long.parseLong(split[1]),
//                            Integer.parseInt(split[2]), split[3], Long.parseLong(split[4])));
//                }).returns(UserBehavior.class);

        SingleOutputStreamOperator<UserBehavior> returns = source.map(line -> {
            String[] split = line.split(",");
            return new UserBehavior(Long.parseLong(split[0]), Long.parseLong(split[1]),
                    Integer.parseInt(split[2]), split[3], Long.parseLong(split[4]));
        }).returns(UserBehavior.class);

        SingleOutputStreamOperator<UserBehavior> filter = returns.filter(t -> "pv".equals(t.getBehavior()));

        filter.map(t -> Tuple2.of(t.getBehavior(), 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(t -> t.f0)
                .sum(1)
                .print();

        env.execute();
    }
}
