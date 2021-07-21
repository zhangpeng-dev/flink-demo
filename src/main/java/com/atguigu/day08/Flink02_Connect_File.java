package com.atguigu.day08;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @ClassName Flink02_Connect_File
 * @Description TODO
 * @Author ASUS
 * @Date 2021/7/21 11:30
 * @Version 1.0
 **/
public class Flink02_Connect_File {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Schema schema = new Schema()
                .field("id", DataTypes.STRING())
                .field("ts", DataTypes.BIGINT())
                .field("vc", DataTypes.INT());

        tableEnv.connect(new FileSystem().path("input/sensor_sql.txt"))
                .withFormat(new Csv())
                .withSchema(schema)
                .createTemporaryTable("sensor");

        Table table = tableEnv.from("sensor")
                .groupBy($("id"))
                .aggregate($("id").count().as("count"))
                .select($("id"), $("count"));

        table.execute().print();

    }
}
