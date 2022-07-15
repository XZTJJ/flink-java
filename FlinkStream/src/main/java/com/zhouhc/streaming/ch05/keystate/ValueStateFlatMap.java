package com.zhouhc.streaming.ch05.keystate;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
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
public class ValueStateFlatMap {
    private final static Logger LOGGER = LoggerFactory.getLogger(ValueStateFlatMap.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        KeyedStream<Tuple2<Integer, Integer>, Integer> keystream = KeyStateBase.before(environment);
        DataStream<Tuple2<Integer, Integer>> flatMapStream = keystream.flatMap(new MyRichFlatMap());
        flatMapStream.print("ValueStateFlatMap");
        environment.execute("ValueStateFlatMap");
    }


    /**
     * 设置过滤函数
     */
    private static class MyRichFlatMap extends RichFlatMapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {
        //创建状态句柄
        private transient ValueState<Tuple2<Integer, Integer>> valueState;

        @Override
        public void open(Configuration parameters) throws Exception {
            LOGGER.info("{} 开始初始化状态", Thread.currentThread().getName());
            //创建文件描述符
            ValueStateDescriptor<Tuple2<Integer, Integer>> stateDescriptor = new ValueStateDescriptor<Tuple2<Integer, Integer>>("uniqueName",
                    TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>() {
                    }));
            //初始化状态
            valueState = getRuntimeContext().getState(stateDescriptor);
        }

        @Override
        public void flatMap(Tuple2<Integer, Integer> value, Collector<Tuple2<Integer, Integer>> out) throws Exception {
            //获取状态
            Tuple2<Integer, Integer> currentSum = valueState.value();
            LOGGER.info("{} currentSum before: {}, input : {}", Thread.currentThread().getName(), currentSum, value);
            System.out.printf("%s currentSum before: %s, input : %s%n", Thread.currentThread().getName(), currentSum, value);
            //赋值或者相加
            if (currentSum == null)
                currentSum = value;
            else
                currentSum.f1 = currentSum.f1 + value.f1;
            //决定是否下发元素
            if (currentSum.f1 % 10 >= 6) {
                out.collect(currentSum);
                valueState.clear();
            } else {
                valueState.update(currentSum);
            }
        }
    }
}
