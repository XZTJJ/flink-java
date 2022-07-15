package com.zhouhc.streaming.ch04.basicOP;

import cn.hutool.core.util.StrUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 最简答的 flatMap 操作
 */
public class FlatMapOP {
    public static void main(String[] args) throws Exception {
        String values = "9 8 7 6 5 4 3 2 1";

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.fromElements(values).flatMap(new FlatMapFunction<String, Long>() {
            @Override
            public void flatMap(String value, Collector<Long> out) throws Exception {
                StrUtil.split(value,' ',-1,true,false).stream()
                        .map(Long::valueOf).filter(temp -> temp > 3).forEach(out::collect);
            }
        }).print("flat map op");
        environment.execute("flat map op");
    }
}
