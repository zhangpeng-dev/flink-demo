package com.atguigu.day06;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @ClassName Flink03_ReducingState
 * @Description TODO
 * @Author ASUS
 * @Date 2021/7/19 13:49
 * @Version 1.0
 **/
public class Flink03_ReducingState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<WaterSensor> map = source.map(line -> {
            String[] split = line.split(",");
            return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));

        }).returns(WaterSensor.class);

        KeyedStream<WaterSensor, String> keyedStream = map.keyBy(WaterSensor::getId);

        //计算每个传感器的水位和
        keyedStream.process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {
            private ReducingState<Integer> sumVC;

            @Override
            public void open(Configuration parameters) throws Exception {
                sumVC = getRuntimeContext().getReducingState(new ReducingStateDescriptor<Integer>(
                        "reducingState", (Integer::sum), Integer.class
                ));
            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                sumVC.add(value.getVc());
                value.setVc(sumVC.get());
                out.collect(value);
            }
        }).print();

        env.execute();
    }
}
