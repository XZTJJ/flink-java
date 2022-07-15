package com.zhouhc.streaming.ch04.taskchainslot;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 *全局禁止的 chain 操作, 不建议
 */
public class GlobalDisableChainTemplate {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度为3
        environment.setParallelism(3);
        //全局精致chain行为
        environment.disableOperatorChaining();
        //默认的source
        DataStream<Tuple2<String, Long>> inputSourceStream = environment.addSource(new ChainSource());
        //filter操作，
        DataStream<Tuple2<String, Long>> fitlerStream = inputSourceStream.filter(new RichFilterFunction<Tuple2<String, Long>>() {
            @Override
            public boolean filter(Tuple2<String, Long> value) throws Exception {
                System.out.printf("filter 所属操作子任务名称 : %s , 元素 : %s %n", getRuntimeContext().getTaskNameWithSubtasks(), value);
                return true;
            }
        });
        //第一个map操作
        DataStream<Tuple2<String, Long>> mapOneStream = fitlerStream.map(new RichMapFunction<Tuple2<String, Long>, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(Tuple2<String, Long> value) throws Exception {
                System.out.printf("map-one 所属操作子任务名称 : %s , 元素 : %s %n", getRuntimeContext().getTaskNameWithSubtasks(), value);
                return value;
            }
        });
        //第二个map操作
        DataStream<Tuple2<String, Long>> mapTwoStream = mapOneStream.map(new RichMapFunction<Tuple2<String, Long>, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(Tuple2<String, Long> value) throws Exception {
                System.out.printf("map-two 所属操作子任务名称 : %s , 元素 : %s %n", getRuntimeContext().getTaskNameWithSubtasks(), value);
                return value;
            }
        });
        //打印操作
        mapTwoStream.print();
        environment.execute("GlobalDisableChain job");
    }
}
