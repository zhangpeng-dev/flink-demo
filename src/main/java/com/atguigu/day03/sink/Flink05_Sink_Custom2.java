package com.atguigu.day03.sink;

import com.atguigu.bean.WaterSensor;
import com.mysql.jdbc.Driver;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName Flink05_Sink_Custom2
 * @Description TODO
 * @Author ASUS
 * @Date 2021/7/14 19:12
 * @Version 1.0
 **/
public class Flink05_Sink_Custom2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<WaterSensor> waterSensorDStream = source.flatMap((FlatMapFunction<String, WaterSensor>) (value, out) -> {
            String[] split = value.split(",");
            out.collect(new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2])));
        }).returns(WaterSensor.class);

        waterSensorDStream.addSink(JdbcSink.sink("insert into sensor values(?,?,?)",
                (JdbcStatementBuilder<WaterSensor>) (ps, t) -> {
                    ps.setString(1, t.getId());
                    ps.setLong(2, t.getTs());
                    ps.setInt(3, t.getVc());
                },
                JdbcExecutionOptions.builder().withBatchSize(1).build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://hadoop102:3306/test?useSSL=false")
                        .withUsername("root")
                        .withPassword("123456")
                        .withDriverName(Driver.class.getName())
                        .build()
                ));

        env.execute();
    }
}
