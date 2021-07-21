package com.atguigu.day08;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @ClassName Flink03_Connect_Kafka
 * @Description TODO
 * @Author ASUS
 * @Date 2021/7/21 11:53
 * @Version 1.0
 **/
public class Flink03_Connect_Kafka {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Schema schema = new Schema()
                .field("id", DataTypes.STRING())
                .field("ts", DataTypes.BIGINT())
                .field("vc", DataTypes.INT());

        tableEnv
                .connect(new Kafka()
                        .version("universal")
                        .topic("sensor")
                        .startFromLatest()
                        .property("group.id", "bigdata")
                        .property("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092"))
                .withFormat(new Json())
                .withSchema(schema)
                .createTemporaryTable("sensor");

        tableEnv
                .from("sensor")
                .groupBy($("id"))
                .select($("id"), $("id").count().as("count"))
                .execute()
                .print();

    }
}
