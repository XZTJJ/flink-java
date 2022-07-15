package com.zhouhc.streaming.ch04.basicOP;


import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * 最简答的 project 操作
 */
public class ProjectOP {
    public static void main(String[] args) throws Exception {
        List<Tuple3<String, Integer, Long>> tuple3List = new ArrayList<Tuple3<String, Integer, Long>>();
        for (int i = 0; i < 10; i++)
            tuple3List.add(new Tuple3<String, Integer, Long>("key" + i, i, Long.valueOf(i)));

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.fromCollection(tuple3List).project(0,2).print("project op");

        environment.execute("project op");
    }
}
