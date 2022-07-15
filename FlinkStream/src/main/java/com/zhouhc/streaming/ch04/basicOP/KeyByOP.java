package com.zhouhc.streaming.ch04.basicOP;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * keyBy操作
 */
public class KeyByOP {
    public static void main(String[] args) throws Exception {
        Tuple3[] tuple3s = new Tuple3[]{
                new Tuple3("key1", 1, 1L),
                new Tuple3("key2", 2, 3L),
                new Tuple3("key1", 3, 3L),
                new Tuple3("key2", 4, 3L),
        };

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.fromElements(tuple3s).keyBy(tuple3 -> tuple3.f0).print("keyBy op");
        environment.execute("keyBy op");
    }
}
