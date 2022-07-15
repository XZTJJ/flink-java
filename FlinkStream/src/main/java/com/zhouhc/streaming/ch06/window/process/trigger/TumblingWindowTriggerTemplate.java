package com.zhouhc.streaming.ch06.window.process.trigger;

import com.zhouhc.streaming.ch06.window.source.SourceForWindow;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;
import java.util.List;


/**
 * 使用自定义的触发策略
 */
public class TumblingWindowTriggerTemplate {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple3<String, Integer, String>> streamSource = environment.addSource(new SourceForWindow(1000, false));
        DataStream<Tuple2<String, List<Integer>>> reduceStream = streamSource.map(new MyMap()).keyBy(item -> item.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .trigger(CountTriggerDebug.of(2L)).reduce(new MyReducingFunction());
        reduceStream.print("TumblingWindowTriggerTemplate");
        environment.execute("TumblingWindowTriggerTemplate");
    }


    /**
     * 指定map方法，主要是将 tuple 转成 List 方便操作
     */
    private static class MyMap implements MapFunction<Tuple3<String, Integer, String>, Tuple2<String, List<Integer>>> {
        @Override
        public Tuple2<String, List<Integer>> map(Tuple3<String, Integer, String> value) throws Exception {
            List<Integer> list = new ArrayList<Integer>();
            list.add(value.f1);
            return Tuple2.of(value.f0, list);
        }
    }

    /**
     * 默认的reduce函数
     */
    private static class MyReducingFunction implements ReduceFunction<Tuple2<String, List<Integer>>> {
        @Override
        public Tuple2<String, List<Integer>> reduce(Tuple2<String, List<Integer>> value1, Tuple2<String, List<Integer>> value2) throws Exception {
            value1.f1.addAll(value2.f1);
            return value1;
        }
    }
}
