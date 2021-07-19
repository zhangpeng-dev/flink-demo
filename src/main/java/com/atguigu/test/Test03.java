package com.atguigu.test;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @ClassName Test03
 * @Description TODO
 * @Author ASUS
 * @Date 2021/7/18 22:39
 * @Version 1.0
 **/
public class Test03 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<WaterSensor> map = source.map(line -> {
            String[] split = line.split(",");
            return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
        }).returns(WaterSensor.class);

        //监控水位传感器的水位值，如果水位值在五秒钟之内(processing time)连续上升，则报警，并将报警信息输出到侧输出流
        SingleOutputStreamOperator<String> process = map
                .assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forMonotonousTimestamps()
                        .withTimestampAssigner((SerializableTimestampAssigner<WaterSensor>) (e, r) -> e.getTs() * 1000))
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    //记录上次水位
                    private Integer lastVC = Integer.MIN_VALUE;
                    //用来记录定时器时间
                    private Long timerTs = Long.MIN_VALUE;

                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                        if (value.getVc() > lastVC) {
                            System.out.println("注册5秒以后的定时器");
                            timerTs = ctx.timestamp() + 5000;
                            ctx.timerService().registerEventTimeTimer(timerTs);
                        } else {
                            System.out.println("删除定时器");
                            ctx.timerService().deleteEventTimeTimer(timerTs);
                            timerTs = Long.MIN_VALUE;
                        }
                        lastVC = value.getVc();
                        out.collect(value.toString());
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        ctx.output(new OutputTag<String>("报警") {
                        }, ctx.getCurrentKey() + "报警");
                        lastVC = Integer.MIN_VALUE;
                    }
                });

        process.print("正常");
        process.getSideOutput(new OutputTag<String>("报警"){}).print("报警");

        env.execute();
    }
}
