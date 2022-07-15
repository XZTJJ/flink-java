package com.zhouhc.streaming.ch06.window.windows;

import com.zhouhc.streaming.ch06.window.source.SourceForWindow;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * 对分组后的全局窗口, 数量和分组了的
 */
public class KeyedGlobalWindow {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple3<String, Integer, String>> streamSource = environment.addSource(new SourceForWindow(1000, true));
        WindowedStream<Tuple3<String, Integer, String>, String, GlobalWindow> globalWindow = streamSource.keyBy(item -> item.f0).window(GlobalWindows.create()).trigger(CountTrigger.of(3));
        globalWindow.sum("f1").print("KeyedGlobalWindow sum");
        environment.execute("KeyedGlobalWindow");
    }
}
