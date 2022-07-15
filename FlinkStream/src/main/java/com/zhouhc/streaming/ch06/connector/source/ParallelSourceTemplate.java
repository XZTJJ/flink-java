package com.zhouhc.streaming.ch06.connector.source;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

//并行数据源，会在job执行的时候，为每一个
//操作符生成一个 source 数据源的
public class ParallelSourceTemplate extends RichParallelSourceFunction<Tuple2<String, Long>> {
    private volatile boolean isRunning = true;
    private long count = 1;
    private final long sleepTime;
    private String sourceFlag;

    public ParallelSourceTemplate(long sleepTime) {
        this.sleepTime = sleepTime;
    }

    //在这里为为每一个数据源进行区分
    @Override
    public void open(Configuration parameters) throws Exception {
        int numberOfParallelSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();
        System.out.printf("当前任务的并行度为%s%n", numberOfParallelSubtasks);
        int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
        if (indexOfThisSubtask == 0)
            sourceFlag = "DB";
        if (indexOfThisSubtask == 1)
            sourceFlag = "MQ";
        super.open(parameters);
    }

    @Override
    public void run(SourceContext<Tuple2<String, Long>> ctx) throws Exception {
        while (isRunning) {
            int numberOfParallelSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();
            System.out.printf("当前任务的并行度为%s%n", numberOfParallelSubtasks);
            count++;
            if (sourceFlag.equals("DB"))
                ctx.collect(new Tuple2<String, Long>(sourceFlag, count));
            if (sourceFlag.equals("MQ"))
                ctx.collect(new Tuple2<String, Long>(sourceFlag, count));
            Thread.sleep(sleepTime);
        }
    }

    @Override
    public void cancel() {
        System.out.printf("关闭数据源%n");
        isRunning = false;
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.addSource(new ParallelSourceTemplate(1000))
                .print("ParallelSourceTemplate");
        env.execute("ParallelSourceTemplate");
    }
}
