package com.atguigu.day03.test;

import com.alibaba.fastjson.JSON;
import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

/**
 * @ClassName Test05
 * @Description TODO
 * @Author ASUS
 * @Date 2021/7/15 10:34
 * @Version 1.0
 **/
public class Test05 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<WaterSensor> returns = source.flatMap((FlatMapFunction<String, WaterSensor>) (value, out) -> {
            String[] split = value.split(",");
            out.collect(new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2])));
        }).returns(WaterSensor.class);

        returns.map(JSON::toJSONString)
                .addSink(new FlinkKafkaProducer<String>("hadoop102", "sensor", new SimpleStringSchema()));

        env.execute();
    }
}
