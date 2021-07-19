package com.atguigu.day06;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @ClassName Flink01_ValueState
 * @Description TODO
 * @Author ASUS
 * @Date 2021/7/19 11:15
 * @Version 1.0
 **/
public class Flink01_ValueState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<WaterSensor> map = source.map(line -> {
            String[] split = line.split(",");
            return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));

        }).returns(WaterSensor.class);

        KeyedStream<WaterSensor, String> keyedStream = map.keyBy(WaterSensor::getId);

        //检测传感器的水位线值，如果连续的两个水位线差值超过10，就输出报警。
        keyedStream.process(new KeyedProcessFunction<String, WaterSensor, String>() {
            private ValueState<Integer> lastVc;

            @Override
            public void open(Configuration parameters) throws Exception {
                lastVc = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("valueState", Integer.class));

            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                Integer last = lastVc.value() == null ? 0 : lastVc.value();
                if (Math.abs(value.getVc() - last) > 10) {
                    out.collect("警报。。。。");
                }
                lastVc.update(value.getVc());
            }
        }).print();

        env.execute();

    }
}
