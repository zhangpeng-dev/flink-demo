package com.atguigu.day02.source;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * @ClassName Flink05_Source_Custom
 * @Description TODO
 * @Author ASUS
 * @Date 2021/7/13 18:12
 * @Version 1.0
 **/
public class Flink05_Source_Custom {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<WaterSensor> streamSource = env.addSource(new MySource());

        streamSource.print();

        env.execute();
    }

    public static class MySource implements SourceFunction<WaterSensor> {
        private Boolean bool = true;
        private Random random = new Random();

        @Override
        public void run(SourceContext<WaterSensor> ctx) throws Exception {
            while (bool) {
                ctx.collect(new WaterSensor("waterSensor" + random.nextInt(10),System.currentTimeMillis(), random.nextInt(1000)));
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            bool = false;
        }
    }
}
