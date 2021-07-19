package com.atguigu.day06;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName Flink02_ListState
 * @Description TODO
 * @Author ASUS
 * @Date 2021/7/19 11:48
 * @Version 1.0
 **/
public class Flink02_ListState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<WaterSensor> map = source.map(line -> {
            String[] split = line.split(",");
            return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));

        }).returns(WaterSensor.class);

        KeyedStream<WaterSensor, String> keyedStream = map.keyBy(WaterSensor::getId);

        //针对每个传感器输出最高的3个水位值
        keyedStream.process(new KeyedProcessFunction<String, WaterSensor, List<Integer>>() {
            private ListState<Integer> top3Vc;

            @Override
            public void open(Configuration parameters) throws Exception {
                top3Vc = getRuntimeContext().getListState(new ListStateDescriptor<Integer>(
                        "listState", Integer.class
                ));
            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<List<Integer>> out) throws Exception {
                top3Vc.add(value.getVc());

                ArrayList<Integer> vcList = new ArrayList<>();
                for (Integer vc : top3Vc.get()) {
                    vcList.add(vc);
                }

                vcList.sort((o1, o2) -> o2 - o1);

                if (vcList.size() > 3) {
                    vcList.remove(3);
                }

                top3Vc.update(vcList);
                out.collect(vcList);
            }
        }).print();

        env.execute();
    }
}
