package com.zhouhc.streaming.ch05.keystate;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
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

import java.util.Iterator;

/**
 * 简单的单值统计信息
 */
public class ReducingStateFlatMap {
    private final static Logger LOGGER = LoggerFactory.getLogger(ReducingStateFlatMap.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        KeyedStream<Tuple2<Integer, Integer>, Integer> keystream = KeyStateBase.before(environment);
        DataStream<Tuple2<Integer, Integer>> flatMapStream = keystream.flatMap(new MyRichFlatMap());
        flatMapStream.print("ReducingStateFlatMap");
        environment.execute("ReducingStateFlatMap");
    }


    /**
     * 设置过滤函数
     */
    private static class MyRichFlatMap extends RichFlatMapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {
        //创建状态句柄
        private transient ReducingState<Tuple2<Integer, Integer>> reducingState;

        @Override
        public void open(Configuration parameters) throws Exception {
            LOGGER.info("{} 开始初始化状态", Thread.currentThread().getName());
            //创建文件描述符
            ReducingStateDescriptor<Tuple2<Integer, Integer>> stateDescriptor = new ReducingStateDescriptor<Tuple2<Integer, Integer>>("uniqueName",
                    new ReduceFunction<Tuple2<Integer, Integer>>() {
                        @Override
                        public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> value1, Tuple2<Integer, Integer> value2) throws Exception {
                            return new Tuple2<Integer, Integer>(value1.f0, value2.f1 + value1.f1);
                        }
                    },
                    TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>() {
                    }));
            //初始化状态
            reducingState = getRuntimeContext().getReducingState(stateDescriptor);
        }

        @Override
        public void flatMap(Tuple2<Integer, Integer> value, Collector<Tuple2<Integer, Integer>> out) throws Exception {
            reducingState.add(value);
            out.collect(reducingState.get());
        }
    }
}
