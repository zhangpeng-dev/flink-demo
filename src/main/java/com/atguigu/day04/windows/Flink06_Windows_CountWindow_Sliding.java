package com.atguigu.day04.windows;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName Flink06_Windows_CountWindow_Sliding
 * @Description TODO
 * @Author ASUS
 * @Date 2021/7/16 19:08
 * @Version 1.0
 **/
public class Flink06_Windows_CountWindow_Sliding {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        source.map(word -> Tuple2.of(word, 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(t -> t.f0)
                .countWindow(10, 3)
                .sum(1)
                .print();

        env.execute();
    }
}
