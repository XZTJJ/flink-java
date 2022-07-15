package com.zhouhc.streaming.ch06.window.process;


import com.zhouhc.streaming.ch06.window.source.SourceForWindow;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 自定义窗口的聚合计算
 */
public class CustomWindowAggregateTemplate {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple3<String, Integer, String>> streamSource = environment.addSource(new SourceForWindow(1000, false));
        DataStream<String> reduceStream = streamSource.keyBy(item -> item.f0).window(TumblingProcessingTimeWindows.of(Time.seconds(5))).aggregate(new MyAggregateFunction());
        reduceStream.print("CustomWindowAggregateTemplate");
        environment.execute("CustomWindowAggregateTemplate");
    }


    //一个自定义的pojo类
    private static class AverageAccumulator {
        private String word = "";
        private long count;
        private long sum;

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public long getCount() {
            return count;
        }

        public void setCount(long count) {
            this.count = count;
        }

        public long getSum() {
            return sum;
        }

        public void setSum(long sum) {
            this.sum = sum;
        }
    }

    /**
     * 自定义 reduce 函数
     */
    private static class MyAggregateFunction implements AggregateFunction<Tuple3<String, Integer, String>, AverageAccumulator, String> {
        //初始默认值
        @Override
        public AverageAccumulator createAccumulator() {
            return new AverageAccumulator();
        }

        //添加新的元素的处理逻辑
        @Override
        public AverageAccumulator add(Tuple3<String, Integer, String> value, AverageAccumulator accumulator) {
            accumulator.setWord(accumulator.getWord() + "_" + value.f0);
            accumulator.setSum(accumulator.getSum() + value.f1);
            accumulator.setCount(accumulator.getCount() + 1);
            return accumulator;
        }

        //结果返回
        @Override
        public String getResult(AverageAccumulator accumulator) {
            return String.format("elem : %s , sum : %s , count : %s", accumulator.getWord(), accumulator.getSum(), accumulator.getCount());
        }

        //两个结果合并
        @Override
        public AverageAccumulator merge(AverageAccumulator a, AverageAccumulator b) {
            a.setWord(a.getWord() + "__" + b.getWord());
            a.setSum(a.getSum() + b.getSum());
            a.setCount(a.getCount() + b.getCount());
            return a;
        }
    }
}
