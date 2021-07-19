package com.atguigu.day02.transform;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @ClassName Flink03_Transform_Rich
 * @Description TODO
 * @Author ASUS
 * @Date 2021/7/13 19:01
 * @Version 1.0
 **/
public class Flink03_Transform_Rich {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<String> flatMap = streamSource.flatMap(new MyFlatMap());

        flatMap.print();

        env.execute();
    }
    public static class MyFlatMap extends RichFlatMapFunction<String,String>{

        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("open..............");
        }

        @Override
        public void close() throws Exception {
            System.out.println("close................");
        }

        @Override
        public void flatMap(String value, Collector<String> out) throws Exception {
            String[] words = value.split(" ");
            for (String word : words) {
                out.collect(word);
            }
        }
    }
}
