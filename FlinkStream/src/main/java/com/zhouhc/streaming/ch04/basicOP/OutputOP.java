package com.zhouhc.streaming.ch04.basicOP;


import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.List;

/**
 * 最简答的 侧边输出 操作
 */
public class OutputOP {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        List<Tuple3<String, Integer, Long>> tuple3List = new ArrayList<Tuple3<String, Integer, Long>>();
        for (int i = 0; i < 10; i++)
            tuple3List.add(new Tuple3<String, Integer, Long>("key" + i, i, Long.valueOf(i)));
        //创建侧边输出
        SingleOutputStreamOperator<Tuple3<String, Integer, Long>> process = environment.fromCollection(tuple3List).process(new ProcessFunction<Tuple3<String, Integer, Long>, Tuple3<String, Integer, Long>>() {
            @Override
            public void processElement(Tuple3<String, Integer, Long> value, ProcessFunction<Tuple3<String, Integer, Long>, Tuple3<String, Integer, Long>>.Context ctx, Collector<Tuple3<String, Integer, Long>> out) throws Exception {
                if (value.f1 % 2 == 0)
                    ctx.output(new OutputTag<Tuple3<String, Integer, Long>>("2file", TypeInformation.of(new TypeHint<Tuple3<String, Integer, Long>>(){})), value);
                if (value.f1 % 3 == 0)
                    ctx.output(new OutputTag<Tuple3<String, Integer, Long>>("3file", TypeInformation.of(new TypeHint<Tuple3<String, Integer, Long>>(){})), value);
                out.collect(value);
            }
        });
        process.getSideOutput(new OutputTag<Tuple3<String, Integer, Long>>("2file", TypeInformation.of(new TypeHint<Tuple3<String, Integer, Long>>(){}))).print("2file");
        process.getSideOutput(new OutputTag<Tuple3<String, Integer, Long>>("3file", TypeInformation.of(new TypeHint<Tuple3<String, Integer, Long>>(){}))).print("3file");
        process.print("mainfile");
        environment.execute("Output op");
    }
}
