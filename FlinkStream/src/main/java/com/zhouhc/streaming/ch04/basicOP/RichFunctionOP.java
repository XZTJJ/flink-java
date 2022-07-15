package com.zhouhc.streaming.ch04.basicOP;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * 简单的复函数实现
 */
public class RichFunctionOP {

    public static void main(String[] args) throws Exception {
        List<Tuple3<String, Integer, Long>> list1 = new ArrayList<Tuple3<String, Integer, Long>>();
        for (int i = 0; i < 2; i++)
            list1.add(new Tuple3<String, Integer, Long>("key1", i, Long.valueOf(i)));

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple3<String, Integer, Long>> list1source = environment.fromCollection(list1);
        list1source.flatMap(new MyRichFlatMap()).print("RichFunction op");
        environment.execute("RichFunction OP");
    }


    /**
     * 复函数实现类
     */
    private static class MyRichFlatMap extends RichFlatMapFunction<Tuple3<String, Integer, Long>, Tuple3<String, Integer, Long>> {
        @Override
        public void open(Configuration parameters) throws Exception {
            RuntimeContext runtimeContext = getRuntimeContext();
            String taskName = runtimeContext.getTaskName();
            String subtasksName = runtimeContext.getTaskNameWithSubtasks();
            int subtaskIndexOf = runtimeContext.getIndexOfThisSubtask();
            int numberOfParallelSubtasks = runtimeContext.getNumberOfParallelSubtasks();
            String job_id = runtimeContext.getJobId().toString();
            System.out.printf("open() 被调用 %n 任务名称(taskName) : %s %n 子任务的名称为(subtasksName) : %s %n", taskName, subtasksName);
            System.out.printf("并行子任务的标识(subtaskIndexOf) : %s %n, 任务(task)的总并行度 : %s %n, job_id : %s %n", subtaskIndexOf, numberOfParallelSubtasks, job_id);
        }

        /**
         * 基本操作， 一般为用户自定逻辑
         */
        @Override
        public void flatMap(Tuple3<String, Integer, Long> value, Collector<Tuple3<String, Integer, Long>> out) throws Exception {
            Thread.sleep(1000);
            out.collect(value);
        }

        @Override
        public void close() throws Exception {
            System.out.println("close() 被调用");
        }
    }
}
