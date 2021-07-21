package com.atguigu.day08;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @ClassName Flink01_SQL_BasicUse
 * @Description TODO
 * @Author ASUS
 * @Date 2021/7/21 11:21
 * @Version 1.0
 **/
public class Flink01_SQL_BasicUse {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<WaterSensor> streamSource = env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("sensor_2", 3000L, 30),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_1", 5000L, 50),
                new WaterSensor("sensor_2", 6000L, 60)
        );

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Table table = tableEnv.fromDataStream(streamSource);

        Table select = table
                .where($("id").isEqual("sensor_1"))
                .select($("id"), $("ts"), $("vc"));


        Table result = table
                .groupBy($("id"))
                .aggregate($("vc").sum().as("vcSum"))
                .select($("id"), $("vcSum"));

//        result.execute().print();

        tableEnv.toAppendStream(select, Row.class).print();

        tableEnv.toRetractStream(result, Row.class).print();

        env.execute();

    }
}
