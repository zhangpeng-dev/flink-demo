package com.atguigu.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @ClassName Flink01_WordCount_Batch
 * @Description TODO
 * @Author ASUS
 * @Date 2021/7/11 16:04
 * @Version 1.0
 **/
public class Flink01_WordCount_Batch {
    public static void main(String[] args) throws Exception {
        //1.创建环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //2.读取数据
        DataSource<String> dataSource = env.readTextFile("E:\\develop\\Java\\flink-demo\\input\\word.txt");

        //3.转换数据格式
        FlatMapOperator<String, Tuple2<String, Long>> wordAndOne = dataSource.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1L));
                }
            }
        });

//        FlatMapOperator<String, Tuple2<String, Long>> wordAndOne = dataSource.flatMap((FlatMapFunction<String, Tuple2<String, Long>>) (value, out) -> {
//            String[] words = value.split(" ");
//            for (String word : words) {
//                out.collect(Tuple2.of(word, 1L));
//            }
//        }).returns(Types.TUPLE(Types.STRING,Types.LONG));

        UnsortedGrouping<Tuple2<String, Long>> wordAndOneUG = wordAndOne.groupBy(0);

        AggregateOperator<Tuple2<String, Long>> sum = wordAndOneUG.sum(1);

        sum.print();
    }
}
