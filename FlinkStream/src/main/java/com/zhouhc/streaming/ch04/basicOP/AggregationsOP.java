package com.zhouhc.streaming.ch04.basicOP;


import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * 最简答的 map 操作
 */
public class AggregationsOP {
    public static void main(String[] args) throws Exception {
        List<Tuple3<String, Integer, Long>> tuple3List = new ArrayList<Tuple3<String, Integer, Long>>();
        tuple3List.add(new Tuple3<String, Integer, Long>("key1", 1, 1L));
        tuple3List.add(new Tuple3<String, Integer, Long>("key1", 5, 5L));
        tuple3List.add(new Tuple3<String, Integer, Long>("key2", 6, 6L));
        tuple3List.add(new Tuple3<String, Integer, Long>("key2", 2, 2L));

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        KeyedStream<Tuple3<String, Integer, Long>, String> tuple3StringKeyedStream = environment.fromCollection(tuple3List).keyBy(tuple3 -> tuple3.f0);
        //注意这里，表明了。流可以多次消费
        tuple3StringKeyedStream.sum(1).print("sum");
        tuple3StringKeyedStream.min(1).print("min");
        tuple3StringKeyedStream.minBy(1).print("minBy");

        environment.execute("Aggregation OP");
    }
}
