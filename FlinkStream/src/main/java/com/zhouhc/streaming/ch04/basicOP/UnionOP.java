package com.zhouhc.streaming.ch04.basicOP;


import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * 最简答的 Union 操作
 */
public class UnionOP {
    public static void main(String[] args) throws Exception {
        List<Tuple3<String, Integer, Long>> list1 = new ArrayList<Tuple3<String, Integer, Long>>();
        List<Tuple3<String, Integer, Long>> list2 = new ArrayList<Tuple3<String, Integer, Long>>();
        for (int i = 0; i < 2; i++) {
            list1.add(new Tuple3<String, Integer, Long>("key1", i, Long.valueOf(i)));
            list2.add(new Tuple3<String, Integer, Long>("key2", i, Long.valueOf(i)));
        }

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple3<String, Integer, Long>> list1source = environment.fromCollection(list1);
        DataStreamSource<Tuple3<String, Integer, Long>> list2source = environment.fromCollection(list2);
        list1source.union(list2source).print("union op");

        environment.execute("union OP");
    }
}
