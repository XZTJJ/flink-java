package com.zhouhc.streaming.ch05.keystate;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
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
public class ListStateFlatMap {
    private final static Logger LOGGER = LoggerFactory.getLogger(ListStateFlatMap.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        KeyedStream<Tuple2<Integer, Integer>, Integer> keystream = KeyStateBase.before(environment);
        DataStream<String> flatMapStream = keystream.flatMap(new MyRichFlatMap());
        flatMapStream.print("ListStateFlatMap");
        environment.execute("ListStateFlatMap");
    }


    /**
     * 设置过滤函数
     */
    private static class MyRichFlatMap extends RichFlatMapFunction<Tuple2<Integer, Integer>, String> {
        //创建状态句柄
        private transient ListState<Tuple2<Integer, Integer>> listState;

        @Override
        public void open(Configuration parameters) throws Exception {
            LOGGER.info("{} 开始初始化状态", Thread.currentThread().getName());
            //创建文件描述符
            ListStateDescriptor<Tuple2<Integer, Integer>> stateDescriptor = new ListStateDescriptor<Tuple2<Integer, Integer>>("uniqueName",
                    TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>() {
                    }));
            //初始化状态
            listState = getRuntimeContext().getListState(stateDescriptor);
        }

        @Override
        public void flatMap(Tuple2<Integer, Integer> value, Collector<String> out) throws Exception {
            listState.add(value);
            int count = 0;
            //遍历并且输出
            Iterator<Tuple2<Integer, Integer>> iterator = listState.get().iterator();
            StringBuffer stringBuffer = new StringBuffer();
            while (iterator.hasNext()) {
                stringBuffer.append(iterator.next()).append(" , ");
                count++;
                //清空数据并且发送
                if (count >= 3) {
                    out.collect(stringBuffer.toString());
                    listState.clear();
                }
            }
        }
    }
}
