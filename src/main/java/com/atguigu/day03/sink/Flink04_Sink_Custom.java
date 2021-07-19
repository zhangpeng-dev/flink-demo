package com.atguigu.day03.sink;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @ClassName Flink04_Sink_custom
 * @Description TODO
 * @Author ASUS
 * @Date 2021/7/14 18:43
 * @Version 1.0
 **/
public class Flink04_Sink_Custom {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<WaterSensor> waterSensorDStream = source.flatMap((FlatMapFunction<String, WaterSensor>) (value, out) -> {
            String[] split = value.split(",");
            out.collect(new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2])));
        }).returns(WaterSensor.class);

        waterSensorDStream.addSink(new MySink());

        env.execute();
    }

    public static class MySink extends RichSinkFunction<WaterSensor> {
        private PreparedStatement ps;
        private Connection conn;

        @Override
        public void open(Configuration parameters) throws Exception {
            conn = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test?useSSL=false", "root", "123456");
            ps = conn.prepareStatement("insert into sensor values (?,?,?)");
        }

        @Override
        public void invoke(WaterSensor value, Context context) throws Exception {
            ps.setString(1, value.getId());
            ps.setLong(2, value.getTs());
            ps.setInt(3, value.getVc());
            ps.execute();
        }

        @Override
        public void close() throws Exception {
            ps.close();
            conn.close();
        }
    }
}
