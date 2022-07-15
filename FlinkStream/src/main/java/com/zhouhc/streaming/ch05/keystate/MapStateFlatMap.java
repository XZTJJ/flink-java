package com.zhouhc.streaming.ch05.keystate;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
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
import java.util.Map;

/**
 * 简单的单值统计信息
 */
public class MapStateFlatMap {
    private final static Logger LOGGER = LoggerFactory.getLogger(MapStateFlatMap.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        KeyedStream<Tuple2<Integer, Integer>, Integer> keystream = KeyStateBase.before(environment);
        DataStream<String> flatMapStream = keystream.flatMap(new MyRichFlatMap());
        flatMapStream.print("MapStateFlatMap");
        environment.execute("MapStateFlatMap");
    }


    /**
     * 设置过滤函数
     */
    private static class MyRichFlatMap extends RichFlatMapFunction<Tuple2<Integer, Integer>, String> {
        //创建状态句柄
        private transient MapState<String, String> mapState;

        @Override
        public void open(Configuration parameters) throws Exception {
            LOGGER.info("{} 开始初始化状态", Thread.currentThread().getName());
            //创建文件描述符
            MapStateDescriptor<String, String> stateDescriptor = new MapStateDescriptor<String, String>("uniqueName",
                    TypeInformation.of(new TypeHint<String>() {
                    }), TypeInformation.of(new TypeHint<String>() {
            }));
            //初始化状态
            mapState = getRuntimeContext().getMapState(stateDescriptor);
        }

        @Override
        public void flatMap(Tuple2<Integer, Integer> value, Collector<String> out) throws Exception {
            if (mapState.contains(value.f0 + "")) {
                out.collect(mapState.get(value.f0 + ""));
            }
            mapState.put(value.f0 + "", value.f1 + "");
        }
    }
}
