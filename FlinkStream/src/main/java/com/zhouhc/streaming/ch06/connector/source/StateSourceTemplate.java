package com.zhouhc.streaming.ch06.connector.source;

import cn.hutool.core.util.StrUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

//并行数据源，会在job执行的时候，为每一个
//操作符生成一个 source 数据源的, 不过 保存了
//source 的一些状态信息，可以防止部分数据重复，
//不过需要加锁，放在多个并行实例同时写入,但是还是会丢一部分数据
// (在执行异步执行检查点和job执行时,会有部分数据丢失的)
public class StateSourceTemplate extends RichParallelSourceFunction<Tuple2<String, Long>> implements CheckpointedFunction {
    private volatile boolean isRunning = true;
    private volatile long count = 1;
    private final long sleepTime;
    private String sourceFlag;
    private final boolean isMarkError;
    //从数据中初始化就好了
    private transient ListState<String> listState;


    public StateSourceTemplate(long sleepTime, boolean isMarkError) {
        this.sleepTime = sleepTime;
        this.isMarkError = isMarkError;
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
        //获取锁对象
        Object checkpointLock = ctx.getCheckpointLock();
        while (isRunning) {
            synchronized (checkpointLock) {
                Tuple2<String, Long> stringLongTuple2 = null;
                if (sourceFlag.equals("DB")) {
                    count = count + 1;
                    stringLongTuple2 = new Tuple2<>(sourceFlag, count);
                }
                if (sourceFlag.equals("MQ")) {
                    count = count + 2;
                    stringLongTuple2 = new Tuple2<>(sourceFlag, count);
                }
                ctx.collect(stringLongTuple2);
                System.out.printf("发送数据元素%s%n", stringLongTuple2);
                int tempI = 0;
                if (isMarkError && System.currentTimeMillis() / 1000 % 60 < 10)
                    tempI = 1 / 0;
                Thread.sleep(sleepTime);
            }
        }
    }

    //取消数据的发送
    @Override
    public void cancel() {
        System.out.printf("关闭数据源%n");
        isRunning = false;
    }

    //执行快照的时候需要删除 状态信息，只需要保存最近的信息就好了
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        listState.clear();
        listState.add(String.format("%s#%s", getRuntimeContext().getIndexOfThisSubtask(), count));
    }

    //从错误中恢复数据
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
//        listState = context.getOperatorStateStore().getUnionListState(new ListStateDescriptor("listsorce", TypeInformation.of(new TypeHint<String>() {})));
        listState = context.getOperatorStateStore().getListState(new ListStateDescriptor("listsorce", TypeInformation.of(new TypeHint<String>() {})));
        if (context.isRestored()) {
            for (String s : listState.get()) {
                System.out.printf("从错误中恢复的状态为%s%n", s);
                List<String> split = StrUtil.split(s, '#', -1, true, true);
                if (getRuntimeContext().getIndexOfThisSubtask() == Integer.valueOf(split.get(0)))
                    count = Long.valueOf(split.get(1));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //保存点设置, 使用新的文件保存方式
        environment.enableCheckpointing(1000);
        String filePath = "file:///F:/logs/flinkcheckpointer";
        environment.setStateBackend(new HashMapStateBackend());
        environment.getCheckpointConfig().setCheckpointStorage(filePath);
        //设置作业失败重启策略
        environment.setRestartStrategy(RestartStrategies.failureRateRestart(3,
                Time.of(5, TimeUnit.MINUTES), Time.of(10, TimeUnit.SECONDS)));

        environment.setParallelism(2);
        DataStream<Tuple2<String, Long>> inputStream = environment.addSource(new StateSourceTemplate(1000, true));

        inputStream.print("StateSourceTemplate");
        environment.execute("StateSourceTemplate");
    }
}
