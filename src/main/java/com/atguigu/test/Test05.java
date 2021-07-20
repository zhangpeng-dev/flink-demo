package com.atguigu.test;

import com.atguigu.day04.bean.OrderEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @ClassName Test05
 * @Description TODO
 * @Author ASUS
 * @Date 2021/7/20 15:40
 * @Version 1.0
 **/
public class Test05 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.readTextFile("input/OrderLog.csv");

        KeyedStream<OrderEvent, Long> keyedStream = streamSource.map(line -> {
            String[] split = line.split(",");
            return new OrderEvent(Long.parseLong(split[0]), split[1], split[2], Long.parseLong(split[3]) * 1000L);
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner((SerializableTimestampAssigner<OrderEvent>)
                        (element, recordTimestamp) -> element.getEventTime()))
                .keyBy(OrderEvent::getOrderId);

        Pattern<OrderEvent, OrderEvent> pattern = Pattern.<OrderEvent>begin("create")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return "create".equals(value.getEventType());
                    }
                })
                .next("pay")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return "pay".equals(value.getEventType());
                    }
                })
                .within(Time.minutes(15));

        SingleOutputStreamOperator<String> result = CEP.pattern(keyedStream, pattern)
                .select(new OutputTag<String>("警报") {
                        },
                        (PatternTimeoutFunction<OrderEvent, String>) (pattern1, timeoutTimestamp) -> pattern1.toString(),
                        (PatternSelectFunction<OrderEvent, String>) Object::toString);

        result.print("正常数据");
        result.getSideOutput(new OutputTag<String>("警报"){}).print("异常数据");

        env.execute();
    }
}
