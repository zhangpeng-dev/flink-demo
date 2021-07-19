package com.atguigu.day04.project;

import com.atguigu.day04.bean.OrderEvent;
import com.atguigu.day04.bean.TxEvent;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName Flink06_Project_Order
 * @Description TODO
 * @Author ASUS
 * @Date 2021/7/15 14:21
 * @Version 1.0
 **/
public class Flink06_Project_Order {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        DataStreamSource<String> orderSource = env.readTextFile("input/OrderLog.csv");

        DataStreamSource<String> logSource = env.readTextFile("input/ReceiptLog.csv");

        SingleOutputStreamOperator<OrderEvent> orderDStream = orderSource.map(line -> {
            String[] split = line.split(",");
            return new OrderEvent(Long.parseLong(split[0]), split[1], split[2], Long.parseLong(split[3]));
        });

        SingleOutputStreamOperator<TxEvent> txDStream = logSource.map(line -> {
            String[] split = line.split(",");
            return new TxEvent(split[0], split[1], Long.parseLong(split[2]));
        });

        ConnectedStreams<OrderEvent, TxEvent> connectedStreams = orderDStream.connect(txDStream);

        ConnectedStreams<OrderEvent, TxEvent> connKeyedDStream = connectedStreams.keyBy("txId", "txId");

        connKeyedDStream.process(new CoProcessFunction<OrderEvent, TxEvent, String>() {
            // 存 txId -> OrderEvent
            Map<String, OrderEvent> orderMap = new HashMap<>();
            // 存储 txId -> TxEvent
            Map<String, TxEvent> txMap = new HashMap<>();

            @Override
            public void processElement1(OrderEvent value, Context ctx, Collector<String> out) throws Exception {
                if (txMap.containsKey(value.getTxId())) {
                    out.collect("订单: " + value + "对账成功");
                    txMap.remove(value.getTxId());
                } else {
                    orderMap.put(value.getTxId(), value);
                }
            }

            @Override
            public void processElement2(TxEvent value, Context ctx, Collector<String> out) throws Exception {
                if (orderMap.containsKey(value.getTxId())) {
                    OrderEvent orderEvent = orderMap.get(value.getTxId());
                    out.collect("订单: " + orderEvent + " 对账成功");
                    orderMap.remove(value.getTxId());
                } else {
                    txMap.put(value.getTxId(), value);
                }
            }
        }).print();

        env.execute();
    }
}
