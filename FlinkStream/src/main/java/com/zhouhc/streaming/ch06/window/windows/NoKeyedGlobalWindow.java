package com.zhouhc.streaming.ch06.window.windows;

import com.zhouhc.streaming.ch06.window.source.SourceForWindow;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

/**
 * 对分组后的全局窗口, 数量和分组了的
 */
public class NoKeyedGlobalWindow {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple3<String, Integer, String>> streamSource = environment.addSource(new SourceForWindow(1000, true));
        AllWindowedStream<Tuple3<String, Integer, String>, GlobalWindow> globalWindow = streamSource.windowAll(GlobalWindows.create()).trigger(CountTrigger.of(3));
        globalWindow.sum("f1").print("NoKeyedGlobalWindow sum");
        environment.execute("NoKeyedGlobalWindow");
    }
}
