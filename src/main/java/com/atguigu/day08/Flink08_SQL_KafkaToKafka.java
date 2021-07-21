package com.atguigu.day08;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Kafka;

/**
 * @ClassName Flink08_SQL_KafkaToKafka
 * @Description TODO
 * @Author ASUS
 * @Date 2021/7/21 18:11
 * @Version 1.0
 **/
public class Flink08_SQL_KafkaToKafka {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv
                .executeSql("create table source_sensor (id String ,ts bigint,vc int) with (" +
                        "'connector' = 'kafka'," +
                        "'topic' = 'topic_source_sensor'," +
                        "'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092'," +
                        "'properties.group.id' = 'bigdata'," +
                        "'scan.startup.mode' = 'latest-offset'," +
                        "'format' = 'csv'" +
                        ")");

        tableEnv
                .executeSql("create table sink_sensor(id string, ts bigint, vc int) with("
                        + "'connector' = 'kafka',"
                        + "'topic' = 'topic_sink_sensor',"
                        + "'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092',"
                        + "'format' = 'csv'"
                        + ")"
                );

        tableEnv.executeSql("insert into sink_sensor select * from source_sensor where id = 's1'");
    }
}
