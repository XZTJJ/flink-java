package com.zhouhc.streaming.ch06.window.windows;

import com.zhouhc.streaming.ch06.window.source.SourceForWindow;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * 滑动的事件窗口
 */
public class SessionEventTimeWindow {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple3<String, Integer, String>> streamSource = environment.addSource(new SourceForWindow(1000, true));
        WindowedStream<Tuple3<String, Integer, String>, String, TimeWindow> windowstream = streamSource.keyBy(item -> item.f0)
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(8)));
        windowstream.sum("f1").print("SessionEventTimeWindow sum");
        environment.execute("SessionEventTimeWindow");
    }
}
