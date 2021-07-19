package com.atguigu.day06;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @ClassName Flink06_ExeByState
 * @Description TODO
 * @Author ASUS
 * @Date 2021/7/19 18:19
 * @Version 1.0
 **/
public class Flink06_ExeByState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<WaterSensor> map = source.map(line -> {
            String[] split = line.split(",");
            return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
        }).returns(WaterSensor.class);

        //监控水位传感器的水位值，如果水位值在五秒钟之内(processing time)连续上升，则报警，并将报警信息输出到侧输出流
        SingleOutputStreamOperator<WaterSensor> process = map.assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forMonotonousTimestamps().withTimestampAssigner(
                (SerializableTimestampAssigner<WaterSensor>) (element, recordTimestamp) -> element.getTs() * 1000
        )).keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {
                    private ValueState<Integer> lastVc;
                    private ValueState<Long> timer;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        lastVc = getRuntimeContext().getState(new ValueStateDescriptor<Integer>(
                                "lastVc", Integer.class, Integer.MIN_VALUE
                        ));
                        timer = getRuntimeContext().getState(new ValueStateDescriptor<Long>(
                                "timer", Long.class, Long.MIN_VALUE
                        ));

                    }

                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                        if (lastVc.value() < value.getVc()) {
                            if (timer.value() == Long.MIN_VALUE) {
                                timer.update(ctx.timestamp() + 5000);
                                System.out.println("注册报警器");
                                ctx.timerService().registerEventTimeTimer(timer.value());
                            }
                        } else {
                            System.out.println("删除报警器");
                            ctx.timerService().deleteEventTimeTimer(timer.value());
                            timer.clear();
                        }
                        lastVc.update(value.getVc());
                        out.collect(value);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<WaterSensor> out) throws Exception {
                        System.out.println("删除报警器");
                        timer.clear();
                        ctx.output(new OutputTag<String>("警报") {
                        }, ctx.getCurrentKey() + ":报警");
                    }
                });

        process.print("正常");
        process.getSideOutput(new OutputTag<String>("警报"){}).print("警报");

        env.execute();
    }
}
