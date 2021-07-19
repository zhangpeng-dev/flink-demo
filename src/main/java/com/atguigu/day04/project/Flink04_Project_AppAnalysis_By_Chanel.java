package com.atguigu.day04.project;

import com.atguigu.day04.bean.MarketingUserBehavior;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * @ClassName Flink04_Project_AppAnalysis_By_Chanel
 * @Description TODO
 * @Author ASUS
 * @Date 2021/7/15 13:49
 * @Version 1.0
 **/
public class Flink04_Project_AppAnalysis_By_Chanel {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<MarketingUserBehavior> source = env.addSource(new AppMarketingDataSource());

        source.map(t -> Tuple2.of(t.getChannel() + "-" + t.getBehavior(), 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(t->t.f0)
                .sum(1)
                .print("channelAndBehavior");

        source.map(behavior->Tuple2.of(behavior.getBehavior(),1L))
                .returns(Types.TUPLE(Types.STRING,Types.LONG))
                .keyBy(t->t.f0)
                .sum(1)
                .print("behavior");

        env.execute();
    }

    public static class AppMarketingDataSource extends RichSourceFunction<MarketingUserBehavior> {
        boolean canRun = true;
        Random random = new Random();
        List<String> channels = Arrays.asList("huawei", "xiaomi", "apple", "baidu", "qq", "oppo", "vivo");
        List<String> behaviors = Arrays.asList("download", "install", "update", "uninstall");

        @Override
        public void run(SourceContext<MarketingUserBehavior> ctx) throws Exception {
            while (canRun) {
                MarketingUserBehavior marketingUserBehavior = new MarketingUserBehavior(
                        (long) random.nextInt(1000000),
                        behaviors.get(random.nextInt(behaviors.size())),
                        channels.get(random.nextInt(channels.size())),
                        System.currentTimeMillis());
                ctx.collect(marketingUserBehavior);
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            canRun = false;
        }
    }
}
