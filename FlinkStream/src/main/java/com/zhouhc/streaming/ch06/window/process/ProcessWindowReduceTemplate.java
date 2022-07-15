package com.zhouhc.streaming.ch06.window.process;

import com.zhouhc.streaming.ch06.window.source.SourceForWindow;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * 窗口函数 和 reduce 函数的结合使用
 */
public class ProcessWindowReduceTemplate {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple3<String, Integer, String>> streamSource = environment.addSource(new SourceForWindow(1000, false));
        DataStream<Tuple2<Long, Tuple3<String, Integer, String>>> reduceStream = streamSource.keyBy(item -> item.f0).window(TumblingProcessingTimeWindows.of(Time.seconds(5))).reduce(new MyReduce(), new MyProcess());
        reduceStream.print("ProcessWindowReduceTemplate");
        environment.execute("ProcessWindowReduceTemplate");
    }


    /**
     * 定义一个 reduce 函数
     */
    private static class MyReduce implements ReduceFunction<Tuple3<String, Integer, String>> {
        @Override
        public Tuple3<String, Integer, String> reduce(Tuple3<String, Integer, String> value1, Tuple3<String, Integer, String> value2) throws Exception {
            return value1.f1 > value2.f1 ? value2 : value1;
        }
    }

    /**
     * 定义一个窗口函数
     */
    private static class MyProcess extends ProcessWindowFunction<Tuple3<String, Integer, String>,
            Tuple2<Long, Tuple3<String, Integer, String>>, String, TimeWindow> {
        //创建一个窗口处理函数
        @Override
        public void process(String key,
                            ProcessWindowFunction<Tuple3<String, Integer, String>, Tuple2<Long, Tuple3<String, Integer, String>>, String, TimeWindow>.Context context,
                            Iterable<Tuple3<String, Integer, String>> elements, Collector<Tuple2<Long, Tuple3<String, Integer, String>>> out) throws Exception {
            Iterator<Tuple3<String, Integer, String>> iterator = elements.iterator();
            //如果是一条一条语句输出的，就证明改窗口是增量聚合的
            while (iterator.hasNext()) {
                Tuple3<String, Integer, String> min = iterator.next();
                Long windowStartTime = context.window().getStart();
                //元数输出
                out.collect(new Tuple2<Long, Tuple3<String, Integer, String>>(windowStartTime,min));
            }
        }
    }
}
