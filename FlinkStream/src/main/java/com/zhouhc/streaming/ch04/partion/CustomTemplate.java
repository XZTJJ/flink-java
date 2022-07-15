package com.zhouhc.streaming.ch04.partion;


import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 自定义分区实现
 */
public class CustomTemplate {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度为3
        environment.setParallelism(3);
        //默认的source
        DataStream<Tuple3<String, Integer, String>> inputSourceStream = environment.addSource(new PartionSource());
        //第一个map操作;
        DataStream<Tuple3<String, Integer, String>> mapOneStream = inputSourceStream.map(new RichMapFunction<Tuple3<String, Integer, String>, Tuple3<String, Integer, String>>() {
            @Override
            public Tuple3<String, Integer, String> map(Tuple3<String, Integer, String> value) throws Exception {
                System.out.printf("元素值 : %s ,分区前子任务名称 : %s ,子任务编号 : %s %n", value,
                        getRuntimeContext().getTaskNameWithSubtasks(), getRuntimeContext().getIndexOfThisSubtask());
                return value;
            }
        });
        //在这里分区
        DataStream<Tuple3<String, Integer, String>> partitionStream = mapOneStream.partitionCustom(new MyPartitioner(), new KeySelector<Tuple3<String, Integer, String>, Tuple3<String, Integer, String>>() {
            @Override
            public Tuple3<String, Integer, String> getKey(Tuple3<String, Integer, String> value) throws Exception {
                return value;
            }
        });
        //分区后的操作
        DataStream<Tuple3<String, Integer, String>> mapTwoStream = partitionStream.map(new RichMapFunction<Tuple3<String, Integer, String>, Tuple3<String, Integer, String>>() {
            @Override
            public Tuple3<String, Integer, String> map(Tuple3<String, Integer, String> value) throws Exception {
                System.out.printf("元素值 : %s ,分区后子任务名称 : %s ,子任务编号 : %s %n", value,
                        getRuntimeContext().getTaskNameWithSubtasks(), getRuntimeContext().getIndexOfThisSubtask());
                return value;
            }
        });
        //打印操作
        mapTwoStream.print();
        environment.execute("CustomTemplate job");
    }
}
