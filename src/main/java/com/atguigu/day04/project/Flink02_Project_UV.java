package com.atguigu.day04.project;

import com.atguigu.day04.bean.UserBehavior;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;

/**
 * @ClassName Flink01_Project_PV
 * @Description TODO
 * @Author ASUS
 * @Date 2021/7/15 10:54
 * @Version 1.0
 **/
public class Flink02_Project_UV {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> source = env.readTextFile("input/UserBehavior.csv");

        SingleOutputStreamOperator<UserBehavior> returns = source.map(line -> {
            String[] split = line.split(",");
            return new UserBehavior(Long.parseLong(split[0]), Long.parseLong(split[1]),
                    Integer.parseInt(split[2]), split[3], Long.parseLong(split[4]));
        }).returns(UserBehavior.class);

        returns.filter(t -> "pv".equals(t.getBehavior()))
                .map(t -> Tuple2.of("uv", t.getUserId()))
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(t -> t.f0)
                .process(new KeyedProcessFunction<String, Tuple2<String, Long>, Long>() {
                    HashSet<Long> set = new HashSet<>();

                    @Override
                    public void processElement(Tuple2<String, Long> value, Context ctx, Collector<Long> out) throws Exception {
                        set.add(value.f1);
                        out.collect((long) set.size());
                    }
                })
                .print("uv");

        env.execute();
    }
}
