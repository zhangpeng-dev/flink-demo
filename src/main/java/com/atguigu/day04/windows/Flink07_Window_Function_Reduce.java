package com.atguigu.day04.windows;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @ClassName Flink07_Window_Function_Reduce
 * @Description TODO
 * @Author ASUS
 * @Date 2021/7/16 19:16
 * @Version 1.0
 **/
public class Flink07_Window_Function_Reduce {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        source.flatMap((FlatMapFunction<String, Tuple2<String, Long>>) (value, out) -> {
            String[] words = value.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(t -> t.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(3)))
                .reduce((ReduceFunction<Tuple2<String, Long>>) (value1, value2) -> Tuple2.of(value1.f0,value1.f1+value2.f1))
                .print();

        env.execute();
    }
}
