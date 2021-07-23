package com.atguigu.day09;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.*;

/**
 * @ClassName Flink05_UDF_TableAgg
 * @Description TODO
 * @Author ASUS
 * @Date 2021/7/23 10:43
 * @Version 1.0
 **/
public class Flink05_UDF_TableAgg {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<WaterSensor> waterSensorStream = streamSource.map(line -> {
            String[] split = line.split(",");
            return new WaterSensor(split[0], Long.parseLong(split[1]) * 1000L, Integer.parseInt(split[2]));
        }).returns(WaterSensor.class)
                .assignTimestampsAndWatermarks(WatermarkStrategy.
                        <WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner((SerializableTimestampAssigner<WaterSensor>)
                                (element, recordTimestamp) -> element.getTs()));

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Table table = tableEnv.fromDataStream(waterSensorStream, $("id"), $("ts"), $("vc"), $("et").rowtime());

        table
                .groupBy($("id"))
                .flatAggregate(call(MyUDTAF.class, $("vc")).as("first", "topN"))
                .select($("id"), $("first"), $("topN")).execute().print();

    }

    public static class Top2Accumulator {
        public Integer first;
        public Integer second;
    }

    public static class MyUDTAF extends TableAggregateFunction<Tuple2<Integer, String>, Top2Accumulator> {
        @Override
        public Top2Accumulator createAccumulator() {
            Top2Accumulator acc = new Top2Accumulator();
            acc.first = Integer.MIN_VALUE;
            acc.second = Integer.MIN_VALUE;
            return acc;
        }

        public void accumulate(Top2Accumulator acc, Integer value) {
            if (acc.first < value) {
                acc.second = acc.first;
                acc.first = value;
            } else if (acc.second < value) {
                acc.second = value;
            }
        }

        public void emitValue(Top2Accumulator acc, Collector<Tuple2<Integer, String>> out) {
            if (acc.first != Integer.MIN_VALUE) {
                out.collect(Tuple2.of(acc.first, "first"));
            }
            if (acc.second != Integer.MIN_VALUE) {
                out.collect(Tuple2.of(acc.second, "second"));
            }
        }

        public void merge(Top2Accumulator acc, Iterable<Top2Accumulator> it) {
            for (Top2Accumulator otherAcc : it) {
                accumulate(acc, otherAcc.first);
                accumulate(acc, otherAcc.second);
            }
        }

    }
}
