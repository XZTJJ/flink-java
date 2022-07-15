package com.zhouhc.streaming.ch05.keystate;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 基本的状态信息
 */
public class KeyStateBase {

    /**
     * 添加数据源
     */
    public static KeyedStream<Tuple2<Integer, Integer>, Integer> before(StreamExecutionEnvironment environment) {
        environment.setParallelism(3);
        KeyedStream<Tuple2<Integer, Integer>, Integer> keyedStream = environment.addSource(new StateSource()).keyBy(
                (Tuple2<Integer, Integer> value) -> value.f0,
                TypeInformation.of(new TypeHint<Integer>() {
                })
        );
        return keyedStream;
    }

    /**
     * 创建数据源
     */
    private static class StateSource implements SourceFunction<Tuple2<Integer, Integer>> {
        public Logger LOG = LoggerFactory.getLogger(StateSource.class);

        private static final long serialVersionUID = 1L;

        private int counter = 0;

        /**
         * 生成元素
         */
        @Override
        public void run(SourceContext<Tuple2<Integer, Integer>> ctx) throws Exception {
            int loopSize = 5;
            while (true) {
                ctx.collect(new Tuple2<Integer, Integer>(counter % loopSize, counter));
                LOG.info("{} send data ({},{})", Thread.currentThread().getName(), counter % loopSize, counter);
                System.out.printf("%s send data (%s,%s)%n", Thread.currentThread().getName(), counter % loopSize, counter);
                counter++;
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {

        }
    }
}
