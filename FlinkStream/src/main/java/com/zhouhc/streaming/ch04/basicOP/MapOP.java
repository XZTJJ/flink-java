package com.zhouhc.streaming.ch04.basicOP;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 最简答的 map 操作
 */
public class MapOP {
    public static void main(String[] args) throws Exception {
        String[] values = new String[]{"9","8","7","6","5"};

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.fromElements(values).map(new MapFunction<String, Long>() {
            @Override
            public Long map(String value) throws Exception {
                return Long.valueOf(value);
            }
        }).print("map op");
        environment.execute("map op");
    }
}
