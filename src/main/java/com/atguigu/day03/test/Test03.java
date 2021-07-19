package com.atguigu.day03.test;

import com.alibaba.fastjson.JSON;
import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.ArrayList;

/**
 * @ClassName Test03
 * @Description TODO
 * @Author ASUS
 * @Date 2021/7/15 10:07
 * @Version 1.0
 **/
public class Test03 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<WaterSensor> returns = source
                .flatMap((FlatMapFunction<String, WaterSensor>) (value, out) -> {
                    String[] split = value.split(",");
                    out.collect(new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2])));
                }).returns(WaterSensor.class);

        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("hadoop102:9200"));
        httpHosts.add(new HttpHost("hadoop103:9200"));
        httpHosts.add(new HttpHost("hadoop104:9200"));

        ElasticsearchSink.Builder<WaterSensor> waterSensorBuilder = new ElasticsearchSink.Builder<>(httpHosts,
                (ElasticsearchSinkFunction<WaterSensor>) (elem, ctx, indexer) -> {
                    indexer.add(new IndexRequest("sensor", "_doc", elem.getId())
                            .source(JSON.toJSONString(elem), XContentType.JSON));
                });

        waterSensorBuilder.setBulkFlushMaxActions(1);

        returns.addSink(waterSensorBuilder.build());

        env.execute();
    }
}
