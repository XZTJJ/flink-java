package com.zhouhc.streaming.ch04.basicOP;

import cn.hutool.core.util.StrUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;


/**
 * 最简答的 Connect, CoMap,  CoFlatMap 操作
 */
public class ConnectCoMapCoFlatMapOP {
    public static void main(String[] args) throws Exception {
        List<Tuple3<String, Integer, Long>> list1 = new ArrayList<Tuple3<String, Integer, Long>>();
        List<Tuple2<String, Integer>> list2 = new ArrayList<Tuple2<String, Integer>>();
        for (int i = 0; i < 1; i++) {
            list1.add(new Tuple3<String, Integer, Long>("key1 key3", i, Long.valueOf(i)));
            list2.add(new Tuple2<String, Integer>("key2 key4", i));
        }
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple3<String, Integer, Long>> list1source = environment.fromCollection(list1);
        DataStreamSource<Tuple2<String, Integer>> list2source = environment.fromCollection(list2);
        //conect
        ConnectedStreams<Tuple3<String, Integer, Long>, Tuple2<String, Integer>> connectStream = list1source.connect(list2source);
        //coMap操作
        connectStream.map(new CoMapFunction<Tuple3<String, Integer, Long>, Tuple2<String, Integer>, Object>() {
            @Override
            public Object map1(Tuple3<String, Integer, Long> value) throws Exception {
                return new Tuple3<String, Integer, Long>("coMap " + value.f0, value.f1, value.f2);
            }

            @Override
            public Object map2(Tuple2<String, Integer> value) throws Exception {
                return new Tuple2<String, Integer>("coMap " + value.f0, value.f1);
            }
        }).print("coMap 操作");
        //CoFlatMap操作
        connectStream.flatMap(new CoFlatMapFunction<Tuple3<String, Integer, Long>, Tuple2<String, Integer>, Object>() {
            @Override
            public void flatMap1(Tuple3<String, Integer, Long> value, Collector<Object> out) throws Exception {
                StrUtil.split(value.f0, ' ', -1, true, false).stream().map(key -> new Tuple3<String, Integer, Long>("coFlatMap " + key, value.f1, value.f2)).forEach(out::collect);
            }

            @Override
            public void flatMap2(Tuple2<String, Integer> value, Collector<Object> out) throws Exception {
                StrUtil.split(value.f0, ' ', -1, true, false).stream().map(key -> new Tuple2<String, Integer>("coFlatMap " + key, value.f1)).forEach(out::collect);
            }
        }).print("coFlatMap 操作");

        environment.execute("ConnectCoMapCoFlatMap OP");
    }
}
