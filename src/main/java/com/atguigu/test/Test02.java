package com.atguigu.test;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @ClassName Test02
 * @Description TODO
 * @Author ASUS
 * @Date 2021/7/18 22:01
 * @Version 1.0
 **/
public class Test02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<WaterSensor> map = source.map(line -> {
            String[] split = line.split(",");
            return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
        }).returns(WaterSensor.class);

        // 采集监控传感器水位值，将水位值高于5cm的值输出到side output
        SingleOutputStreamOperator<WaterSensor> process = map.process(new ProcessFunction<WaterSensor, WaterSensor>() {
            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                if (value.getVc() <= 5) {
                    out.collect(value);
                } else {
                    ctx.output(new OutputTag<WaterSensor>("警告") {
                    }, value);
                }
            }
        });

        process.print("正常");
        process.getSideOutput(new OutputTag<WaterSensor>("警告"){}).print("警告");

        env.execute();
    }
}
