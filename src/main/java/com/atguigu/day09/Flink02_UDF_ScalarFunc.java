package com.atguigu.day09;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @ClassName Flink02_UDF_ScalarFunc
 * @Description TODO
 * @Author ASUS
 * @Date 2021/7/22 15:19
 * @Version 1.0
 **/
public class Flink02_UDF_ScalarFunc {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> waterSensorStream = env.socketTextStream("hadoop102", 9999)
                .map(line -> {
                    String[] split = line.split(",");
                    return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                }).returns(WaterSensor.class);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Table table = tableEnv.fromDataStream(waterSensorStream);

        table.select($("id"));


    }

    public static class MyUDFFunc extends ScalarFunction {
        public int eval(String value) {
            return value.length();
        }
    }
}
