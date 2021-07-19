package com.atguigu.day02.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;

/**
 * @ClassName Flink05_Source_Kafka
 * @Description TODO
 * @Author ASUS
 * @Date 2021/7/13 16:41
 * @Version 1.0
 **/
public class Flink04_Source_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092");
        properties.setProperty("group.id", "Flink01_Source_Kafka");
        properties.setProperty("auto.offset.reset", "latest");

        DataStreamSource<String> streamSource = env.addSource(new FlinkKafkaConsumer<>("senor",
                new SimpleStringSchema(), properties));

        streamSource.print();

        env.execute();
    }
}