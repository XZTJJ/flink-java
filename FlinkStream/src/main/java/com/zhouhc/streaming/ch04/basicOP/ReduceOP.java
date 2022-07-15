package com.zhouhc.streaming.ch04.basicOP;


import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * 最简答的 reduce 操作
 */
public class ReduceOP {
    public static void main(String[] args) throws Exception {
        List<Tuple3<String, Integer, Long>> tuple3List = new ArrayList<Tuple3<String, Integer, Long>>();
        tuple3List.add(new Tuple3<String, Integer, Long>("key1", 1, 1L));
        tuple3List.add(new Tuple3<String, Integer, Long>("key2", 2, 2L));
        tuple3List.add(new Tuple3<String, Integer, Long>("key1", 3, 3L));
        tuple3List.add(new Tuple3<String, Integer, Long>("key2", 4, 3L));

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.fromCollection(tuple3List).keyBy(tuple3 -> tuple3.f0)
                .reduce((value1, value2) -> {
                    System.out.printf("Thread name is %s , value1: %s ,value2: %s %n", Thread.currentThread().getName(), value1, value2);
                    return new Tuple3<String, Integer, Long>(value1.f0, value1.f1 + value2.f1, value1.f2 + value2.f2);
                }).print("Reduce OP");

        environment.execute("Reduce OP");
    }
}
