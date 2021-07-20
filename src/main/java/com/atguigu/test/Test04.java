package com.atguigu.test;

import com.atguigu.bean.LoginEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @ClassName Test04
 * @Description TODO
 * @Author ASUS
 * @Date 2021/7/20 15:04
 * @Version 1.0
 **/
public class Test04 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.readTextFile("input/LoginLog.csv");

        KeyedStream<LoginEvent, Long> keyedStream = streamSource.map(line -> {
            String[] split = line.split(",");
            return new LoginEvent(Long.parseLong(split[0]), split[1], split[2], Long.parseLong(split[3]) * 1000L);
        }).returns(LoginEvent.class)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner((SerializableTimestampAssigner<LoginEvent>)
                                (element, recordTimestamp) -> element.getEventTime()))
                .keyBy(LoginEvent::getUserId);

        Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>begin("begin")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return "fail".equals(value.getEventType());
                    }
                })
                .times(2)
                .consecutive()
                .within(Time.seconds(2));

        CEP.pattern(keyedStream, pattern)
                .select((PatternSelectFunction<LoginEvent, String>) Object::toString).print();

        env.execute();
    }
}
