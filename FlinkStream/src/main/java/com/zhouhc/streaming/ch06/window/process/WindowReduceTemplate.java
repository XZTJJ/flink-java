package com.zhouhc.streaming.ch06.window.process;

import com.zhouhc.streaming.ch06.window.source.SourceForWindow;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;


/**
 * 窗口处理函数
 */
public class WindowReduceTemplate {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple3<String, Integer, String>> streamSource = environment.addSource(new SourceForWindow(1000, false));
        DataStream<Tuple3<String, Integer, String>> reduceStream = streamSource.keyBy(item -> item.f0).window(TumblingProcessingTimeWindows.of(Time.seconds(5))).reduce(new MyMiniReduce());
        reduceStream.print("WindowReduceTemplate");
        environment.execute("WindowReduceTemplate");
    }


    /**
     * 自定义 reduce 函数
     */
    private static class MyMiniReduce implements ReduceFunction<Tuple3<String, Integer, String>> {
        @Override
        public Tuple3<String, Integer, String> reduce(Tuple3<String, Integer, String> value1, Tuple3<String, Integer, String> value2) throws Exception {
            return value1.f1 > value2.f1 ? value2 : value1;
        }
    }
}
