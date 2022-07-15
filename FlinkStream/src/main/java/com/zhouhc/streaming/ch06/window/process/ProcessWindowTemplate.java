package com.zhouhc.streaming.ch06.window.process;

import com.zhouhc.streaming.ch06.window.source.SourceForWindow;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 对同一个窗口中的元素进行统计和计算
 */
public class ProcessWindowTemplate {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple3<String, Integer, String>> streamSource = environment.addSource(new SourceForWindow(1000, false));
        DataStream<String> reduceStream = streamSource.keyBy(item -> item.f0).window(TumblingProcessingTimeWindows.of(Time.seconds(5))).process(new MyProcessWindow());
        reduceStream.print("ProcessWindowTemplate");
        environment.execute("ProcessWindowTemplate");
    }


    /**
     * 自定义窗口出来函数
     */
    private static class MyProcessWindow extends ProcessWindowFunction<Tuple3<String, Integer, String>, String, String, TimeWindow> {
        //元素内容
        private StringBuffer stringBuffer = new StringBuffer();
        //元素个数
        private long count;

        //迭代并且遍历元素,统计元素的个数
        @Override
        public void process(String key, ProcessWindowFunction<Tuple3<String, Integer, String>, String, String, TimeWindow>.Context context,
                            Iterable<Tuple3<String, Integer, String>> elements, Collector<String> out) throws Exception {
            for (Tuple3<String, Integer, String> element : elements) {
                count++;
                stringBuffer.append(elements);
            }
            System.out.printf("窗口内元素为 : %s%n",stringBuffer.toString());
            out.collect(String.format("window : %s , key : %s , count : %s", context.window(), key, count));
        }

    }
}
