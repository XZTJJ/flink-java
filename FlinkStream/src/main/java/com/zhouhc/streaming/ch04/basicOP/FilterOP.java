package com.zhouhc.streaming.ch04.basicOP;

import cn.hutool.core.util.StrUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 最简答的 flatMap 操作
 */
public class FilterOP {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.fromSequence(1,5).filter(value -> value % 2 == 0).print("filter op");
        environment.execute("filter op");
    }
}
