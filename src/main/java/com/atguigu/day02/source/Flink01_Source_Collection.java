package com.atguigu.day02.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * @ClassName Flink01_Source_Collection
 * @Description TODO
 * @Author ASUS
 * @Date 2021/7/13 16:29
 * @Version 1.0
 **/
public class Flink01_Source_Collection {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        ArrayList<String> list = new ArrayList<>();
        list.add("101");
        list.add("102");
        list.add("103");
        DataStreamSource<String> streamSource = env.fromCollection(list);

        streamSource.print();

        env.execute();
    }
}
