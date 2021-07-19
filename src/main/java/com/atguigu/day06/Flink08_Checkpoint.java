package com.atguigu.day06;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName Flink08_Checkpoint
 * @Description TODO
 * @Author ASUS
 * @Date 2021/7/19 20:20
 * @Version 1.0
 **/
public class Flink08_Checkpoint {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        System.setProperty("HADOOP_USER_NAME", "atguigu");

        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/ck"));

        env.enableCheckpointing(5000);

        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        source.flatMap((FlatMapFunction<String, Tuple2<String, Long>>) (line, out) -> {
            String[] words = line.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(w -> w.f0)
                .sum(1)
                .print();

        env.execute();
    }
}
