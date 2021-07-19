package com.atguigu.day02.transform;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName Flink01_Transform_Map
 * @Description TODO
 * @Author ASUS
 * @Date 2021/7/13 18:43
 * @Version 1.0
 **/
public class Flink01_Transform_Map {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        streamSource.map(word->new WaterSensor(word.split(",")[0],Long.parseLong(word.split(",")[1])
                ,Integer.parseInt(word.split(",")[2]))).print();

        env.execute();
    }
}
