package com.atguigu.day09;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * @ClassName Flink04_UDF_Agg
 * @Description TODO
 * @Author ASUS
 * @Date 2021/7/23 10:19
 * @Version 1.0
 **/
public class Flink04_UDF_Agg {
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

//        table
//                .groupBy($("id"))
//                .select($("id"), call(MyUDAF.class, $("vc"))).execute().print();

        tableEnv.createFunction("myAvg", MyUDAF.class);

        tableEnv.executeSql("select id,myAvg(vc) from " + table + " group by id").print();
    }

    public static class WeightedAvgAccumulator {
        public long vcSum = 0;
        public int count = 0;
    }

    public static class MyUDAF extends AggregateFunction<Double, WeightedAvgAccumulator> {

        @Override
        public Double getValue(WeightedAvgAccumulator accumulator) {
            return accumulator.vcSum * 1D / accumulator.count;
        }

        @Override
        public WeightedAvgAccumulator createAccumulator() {
            return new WeightedAvgAccumulator();
        }

        public void accumulate(WeightedAvgAccumulator acc, Long iValue) {
            acc.vcSum += iValue;
            acc.count += 1;
        }
    }
}
