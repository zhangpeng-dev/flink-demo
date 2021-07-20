package com.atguigu.day06;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @ClassName Flink05_MapState
 * @Description TODO
 * @Author ASUS
 * @Date 2021/7/19 13:50
 * @Version 1.0
 **/
public class Flink05_MapState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);



        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<WaterSensor> map = source.map(line -> {
            String[] split = line.split(",");
            return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));

        }).returns(WaterSensor.class);

        KeyedStream<WaterSensor, String> keyedStream = map.keyBy(WaterSensor::getId);

        //去重: 去掉重复的水位值. 思路: 把水位值作为MapState的key来实现去重, value随意
        keyedStream.process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {
            private MapState<Integer, WaterSensor> mapState;

            @Override
            public void open(Configuration parameters) throws Exception {
                mapState = getRuntimeContext().getMapState(new MapStateDescriptor<Integer, WaterSensor>(
                        "mapState", Integer.class, WaterSensor.class
                ));
            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                if (!mapState.contains(value.getVc())){
                    mapState.put(value.getVc(),value);
                    out.collect(value);
                }
            }
        }).print();

        env.execute();
    }
}
