package com.atguigu.day03.sink;

import com.alibaba.fastjson.JSON;
import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @ClassName Flink02_Sink_Redis
 * @Description TODO
 * @Author ASUS
 * @Date 2021/7/14 16:53
 * @Version 1.0
 **/
public class Flink02_Sink_Redis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<WaterSensor> waterSensor = source.flatMap((FlatMapFunction<String, WaterSensor>) (line, out) -> {
            String[] split = line.split(",");
            out.collect(new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2])));
        }).returns(WaterSensor.class);

        FlinkJedisPoolConfig flinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder().setHost("hadoop102").build();

        waterSensor.addSink(new RedisSink<WaterSensor>(flinkJedisPoolConfig, new RedisMapper<WaterSensor>() {
            @Override
            public RedisCommandDescription getCommandDescription() {
                return new RedisCommandDescription(RedisCommand.HSET, "Hash");
            }

            @Override
            public String getKeyFromData(WaterSensor data) {
                return data.getId();
            }

            @Override
            public String getValueFromData(WaterSensor data) {
                return JSON.toJSONString(data);
            }
        }));

        env.execute();
    }
}