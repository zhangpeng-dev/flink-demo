package com.atguigu.day09;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @ClassName Flink01_OverWindow
 * @Description TODO
 * @Author ASUS
 * @Date 2021/7/22 10:44
 * @Version 1.0
 **/
public class Flink01_OverWindow {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> waterSensorStream = env.fromElements(
                new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("sensor_2", 3000L, 30),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_1", 5000L, 50),
                new WaterSensor("sensor_2", 6000L, 60)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner((SerializableTimestampAssigner<WaterSensor>)
                        (element, recordTimestamp) -> element.getTs()));

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Table table = tableEnv.fromDataStream(waterSensorStream, $("id"), $("ts"), $("vc"), $("et").rowtime());

        tableEnv.createTemporaryView("sensor", table);

        tableEnv.executeSql("select id ,sum(vc) over(partition by id order by et) from sensor").print();

        tableEnv.executeSql("select id,sum(vc) sumVc from sensor group by tumble(et,interval '2' second),id").print();

        tableEnv.executeSql("select id,sum(vc) from sensor group by hop(et,interval '2' second,interval '4' second),id").print();

    }
}
