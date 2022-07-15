package com.zhouhc.streaming.ch05.operatorstate;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.LinkedList;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * 整个操作符进行序列化, 可以使用  Even-Split重新分配模式 或者 Union 重新分配模式,
 * 因此可以选择使用何种方式处理, 本实例使用的是 true 和 false开控制选择
 * 如果不想丢失任何状态的话，就把每个元素放入检查点，而不是JVM中暂存元素。
 */
public class CheckpointedMapTemplate {

    //初始化环境和环境变量
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //检查点设置,设置全局并行度为2
        environment.enableCheckpointing(10000);
        environment.setParallelism(2);
        //保存点设置, 使用新的文件保存方式
        String filePath = "file:///F:/logs/flinkcheckpointer";
        environment.setStateBackend(new HashMapStateBackend());
        environment.getCheckpointConfig().setCheckpointStorage(filePath);
        //设置作业失败重启策略
        environment.setRestartStrategy(RestartStrategies.failureRateRestart(3,
                Time.of(5, TimeUnit.MINUTES), Time.of(10, TimeUnit.SECONDS)));
        //使用自定义数据源，并且设置数据源并行度为1
        DataStream<Long> sourceStream = environment.addSource(new CustomSource()).setParallelism(1);
        DataStream<String> mapStream = sourceStream.map(new MyStateMap(false, true));
        mapStream.print("CheckpointedMapTemplate");
        environment.execute("CheckpointedMapTemplate");
    }

    /**
     * 序列化Map操作,模拟出现异常的情况
     */
    private static class MyStateMap implements MapFunction<Long, String>, CheckpointedFunction {
        //默认的状态类型，目前都是 ListState
        private transient ListState<Long> checkpointer;
        //本地保存的状态
        private LinkedList<Long> bufferedElements;
        //布尔值,是否重新分配
        private boolean isUnion;
        //是否模拟产生异常
        private boolean isError;

        //传递对应的标识
        public MyStateMap(boolean isUnion, boolean isError) {
            this.isUnion = isUnion;
            this.isError = isError;
            bufferedElements = new LinkedList<Long>();
        }

        /**
         * 默认的操作
         */
        @Override
        public String map(Long value) throws Exception {
            bufferedElements.add(value);
            //如果大小超过10,就删除最新的元素
            if (bufferedElements.size() > 10)
                bufferedElements.pollFirst();
            //是否产生错误
            if (isError) {
                int second = (int) (System.currentTimeMillis() / 1000 % 60);
                if (second >= 50 && second <= 51)
                    second = 1 / 0;
            }
            System.out.printf("%s : map data : %s %n", Thread.currentThread().getName(),
                    bufferedElements.stream().map(String::valueOf).collect(Collectors.joining(",")));
            return String.format("集合的首尾元素为:( %s, %s ) , 集合大小为 : %s%n", bufferedElements.getFirst(), bufferedElements.getLast(), bufferedElements.size());
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            System.out.printf("%s save snapshotState checkpointedId: %s ,checktime: %s , save data is : %s %n",
                    Thread.currentThread().getName(), context.getCheckpointId(), context.getCheckpointTimestamp(),
                    bufferedElements.stream().map(String::valueOf).collect(Collectors.joining(",")));
            //清空旧状态，保存信状态
            checkpointer.clear();
            checkpointer.addAll(bufferedElements);
        }

        //初始化或者从错误中恢复的操作
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            //创建描述符
            ListStateDescriptor<Long> stateDescriptor = new ListStateDescriptor<Long>("listunname",
                    TypeInformation.of(new TypeHint<Long>() {
                    }));
            if (isUnion)
                checkpointer = context.getOperatorStateStore().getUnionListState(stateDescriptor);
            else
                checkpointer = context.getOperatorStateStore().getListState(stateDescriptor);
            //是否从错误中恢复
            if (context.isRestored()) {
                Iterable<Long> longIterable = checkpointer.get();
                //bufferedElements初始化
                longIterable.forEach(bufferedElements::add);
                System.out.printf("%s operator状态恢复 %s%n",
                        Thread.currentThread().getName(),
                        bufferedElements.stream().map(String::valueOf).collect(Collectors.joining(",")));
            }
        }
    }
}
