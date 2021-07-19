package com.atguigu.day04.project;

import com.atguigu.day04.bean.AdsClickLog;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName Flink06_Project_Ads_Click
 * @Description TODO
 * @Author ASUS
 * @Date 2021/7/15 14:05
 * @Version 1.0
 **/
public class Flink05_Project_Ads_Click {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> source = env.readTextFile("input/AdClickLog.csv");

        source
                .map(line -> {
                    String[] split = line.split(",");
                    return new AdsClickLog(Long.parseLong(split[0]), Long.parseLong(split[1]), split[2],
                            split[3], Long.parseLong(split[4]));
                })
                .map(log-> Tuple2.of(Tuple2.of(log.getProvince(),log.getAdId()),1L))
                .returns(Types.TUPLE(Types.TUPLE(Types.STRING,Types.LONG),Types.LONG))
                .keyBy(new KeySelector<Tuple2<Tuple2<String, Long>, Long>, Tuple2<String,Long>>() {
                    @Override
                    public Tuple2<String, Long> getKey(Tuple2<Tuple2<String, Long>, Long> value) throws Exception {
                        return value.f0;
                    }
                })
                .sum(1)
                .print("省份-广告");

        env.execute();
    }
}
