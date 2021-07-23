package com.atguigu.day09;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.*;

/**
 * @ClassName Flink03_UDF_Table
 * @Description TODO
 * @Author ASUS
 * @Date 2021/7/23 9:40
 * @Version 1.0
 **/
public class Flink03_UDF_Table {
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
//                .joinLateral(call(MyUDTF.class,$("id")))
//                .select($("id"),$("word")).execute().print();

        tableEnv.createFunction("myUDTF",MyUDTF.class);

//        table
//                .joinLateral(call("myUDTF",$("id")))
//                .select($("id"),$("word")).execute().print();

        tableEnv.executeSql("select id,word from "+table+
                " left join lateral table(myUDTF(id)) on true").print();

    }

    @FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
    public static class MyUDTF extends TableFunction<Row> {
        public void eval(String str){
            String[] s = str.split("_");
            for (String s1 : s) {
                collect(Row.of(s1));
            }
        }
    }

}
