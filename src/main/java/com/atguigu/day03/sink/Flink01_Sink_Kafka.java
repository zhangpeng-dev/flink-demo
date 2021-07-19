package com.atguigu.day03.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

/**
 * @ClassName Flink01_Transform_Kafka
 * @Description TODO
 * @Author ASUS
 * @Date 2021/7/14 11:51
 * @Version 1.0
 **/
public class Flink01_Sink_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        streamSource.addSink(new FlinkKafkaProducer<String>("hadoop102:9092","sensor",new SimpleStringSchema()));

        env.execute();
    }
}