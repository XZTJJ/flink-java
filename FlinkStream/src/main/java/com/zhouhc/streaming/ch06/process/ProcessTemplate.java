package com.zhouhc.streaming.ch06.process;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Random;

/**
 * 测试 process操作 符
 */
public class ProcessTemplate {

    //设置对应的数据
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple2<String, Long>> processStream = environment.addSource(new ProcessSource())
                .assignTimestampsAndWatermarks(getCurrentWatermark())
                .keyBy(item -> item.f0)
                .process(new SessionProcess());
        processStream.print("ProcessTemplate");
        environment.execute("ProcessTemplate");
    }


    //自定义水印生成策略
    private static WatermarkStrategy<Tuple2<String, Long>> getCurrentWatermark() {
        return WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                .withTimestampAssigner((Tuple2<String, Long> element, long recordTimestamp) -> element.f1);
    }

    //定义需要的pojo类
    private static class CountWithTimestamp {

        public String key;

        public long count;

        public long lastModified;

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public long getCount() {
            return count;
        }

        public void setCount(long count) {
            this.count = count;
        }

        public long getLastModified() {
            return lastModified;
        }

        public void setLastModified(long lastModified) {
            this.lastModified = lastModified;
        }
    }

    //自定义一个数据源
    private static class ProcessSource implements SourceFunction<Tuple2<String, Long>> {
        private static final long serialVersionUID = 1L;

        final String[] strings = new String[]{"flink", "streaming", "java"};

        @Override
        public void run(SourceContext<Tuple2<String, Long>> ctx) throws Exception {
            int number = 0;
            while (true) {
                Thread.sleep(1000);
                int index = new Random().nextInt(3);
                long time = System.currentTimeMillis();
                Tuple2<String, Long> tuple2 = new Tuple2<>(strings[index], time);
                ctx.collect(tuple2);
                System.out.println("发送元素:" + tuple2);
                number++;
                if (number % 10 > 3 && number % 10 < 6) {
                    Thread.sleep(12000);
                }
            }
        }

        @Override
        public void cancel() {
        }
    }

    //自定义一个需要用到的 process 函数
    private static class SessionProcess extends KeyedProcessFunction<String, Tuple2<String, Long>, Tuple2<String, Long>> {

        private ValueState<CountWithTimestamp> valueState;

        //获取对应的状态
        @Override
        public void open(Configuration parameters) throws Exception {
            final ValueStateDescriptor<CountWithTimestamp> stateDescriptor = new ValueStateDescriptor(
                    "timeKeyValue", TypeInformation.of(new TypeHint<CountWithTimestamp>() {
            })
            );
            valueState = getRuntimeContext().getState(stateDescriptor);
        }

        //设置处理逻辑，超过10s没有发送的元素触发定时器
        @Override
        public void processElement(Tuple2<String, Long> value,
                                   KeyedProcessFunction<String, Tuple2<String, Long>, Tuple2<String, Long>>.Context ctx,
                                   Collector<Tuple2<String, Long>> out) throws Exception {
            CountWithTimestamp currentState = valueState.value();
            //设置状态值
            if (currentState == null) {
                currentState = new CountWithTimestamp();
                currentState.key = value.f0;
            }
            currentState.count++;
            currentState.lastModified = ctx.timestamp();
            valueState.update(currentState);
            //获取事件时间注册器
            TimerService timerService = ctx.timerService();
            timerService.registerEventTimeTimer(currentState.lastModified + 10000);
        }

        //触发 事件注册器，并且往下游发送元素
        @Override
        public void onTimer(long timestamp,
                            KeyedProcessFunction<String, Tuple2<String, Long>, Tuple2<String, Long>>.OnTimerContext ctx,
                            Collector<Tuple2<String, Long>> out) throws Exception {
            CountWithTimestamp value = valueState.value();
            if (value.lastModified + 10000 == timestamp) {
                out.collect(new Tuple2<String, Long>(value.key, value.count));
            }
        }
    }
}
