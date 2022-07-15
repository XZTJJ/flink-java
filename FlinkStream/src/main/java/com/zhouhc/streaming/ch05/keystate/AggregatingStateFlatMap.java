package com.zhouhc.streaming.ch05.keystate;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 简单的单值统计信息
 */
public class AggregatingStateFlatMap {
    private final static Logger LOGGER = LoggerFactory.getLogger(AggregatingStateFlatMap.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        KeyedStream<Tuple2<Integer, Integer>, Integer> keystream = KeyStateBase.before(environment);
        DataStream<Long> flatMapStream = keystream.flatMap(new MyRichFlatMap());
        flatMapStream.print("AggregatingStateFlatMap");
        environment.execute("AggregatingStateFlatMap");
    }


    /**
     * 设置过滤函数
     */
    private static class MyRichFlatMap extends RichFlatMapFunction<Tuple2<Integer, Integer>, Long> {
        //创建状态句柄
        private transient AggregatingState<Tuple2<Integer, Integer>, Long> aggregatingState;

        @Override
        public void open(Configuration parameters) throws Exception {
            LOGGER.info("{} 开始初始化状态", Thread.currentThread().getName());
            //创建文件描述符
            AggregatingStateDescriptor<Tuple2<Integer, Integer>, Long, Long> stateDescriptor = new AggregatingStateDescriptor<Tuple2<Integer, Integer>, Long, Long>(
                    "uniqueName",
                    new AggregateFunction<Tuple2<Integer, Integer>, Long, Long>() {
                        @Override
                        public Long createAccumulator() {
                            return 0L;
                        }

                        @Override
                        public Long add(Tuple2<Integer, Integer> value, Long accumulator) {
                            return accumulator + value.f1;
                        }

                        @Override
                        public Long getResult(Long accumulator) {
                            return accumulator;
                        }

                        @Override
                        public Long merge(Long a, Long b) {
                            return a + b;
                        }
                    },
                    TypeInformation.of(new TypeHint<Long>() {
                    }));
            //初始化状态
            aggregatingState = getRuntimeContext().getAggregatingState(stateDescriptor);
        }

        @Override
        public void flatMap(Tuple2<Integer, Integer> value, Collector<Long> out) throws Exception {
            //获取状态
            aggregatingState.add(value);
            out.collect(aggregatingState.get());
        }
    }
}
