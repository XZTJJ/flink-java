package com.zhouhc.streaming.ch06.window.process.trigger;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;

import java.util.List;


/**
 * 自定义窗口触发逻辑
 */
public class CountTriggerDebug<W extends Window> extends Trigger<Tuple2<String, List<Integer>>, W> {

    //指定窗口元素最大值
    private final long maxCount;

    //定义一个状态描述符
    private final ReducingStateDescriptor<Long> stateDescriptor =
            new ReducingStateDescriptor<Long>("count", new SUM(),
                    TypeInformation.of(new TypeHint<Long>() {
                    }));

    //构造函数
    private CountTriggerDebug(long maxCount) {
        this.maxCount = maxCount;
    }

    @Override
    public TriggerResult onElement(Tuple2<String, List<Integer>> element, long timestamp, W window, TriggerContext ctx) throws Exception {
        //获取任务的状态
        ReducingState<Long> count = ctx.getPartitionedState(stateDescriptor);
        count.add(1L);
        if (count.get() >= maxCount) {
            System.out.printf("触发器触发窗口函数对该窗口计算,同时清除该窗口的技术状态,--%s%n", count.get());
            count.clear();
//            return TriggerResult.FIRE;
            return TriggerResult.FIRE_AND_PURGE;
        }
        System.out.printf("触发器仅对该窗口的技术状态进行加一操作--%s%n", count.get());
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, W window, TriggerContext ctx) throws Exception {
        System.out.printf("触发器调用 onProcessingTime 方法%n");
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, W window, TriggerContext ctx) throws Exception {
        System.out.printf("触发器调用 onEventTime 方法%n");
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(W window, TriggerContext ctx) throws Exception {
        System.out.printf("触发器调用 clear 方法%n");
        ctx.getPartitionedState(stateDescriptor).clear();
    }

    @Override
    public boolean canMerge() {
        return true;
    }

    @Override
    public void onMerge(W window, OnMergeContext ctx) throws Exception {
        System.out.printf("触发器调用onMerge方法%n");
        ctx.mergePartitionedState(stateDescriptor);
    }

    @Override
    public String toString() {
        return "CountTrigger(" + maxCount + ")";
    }

    //获取实例方法
    public static <W extends Window> CountTriggerDebug<W> of(long maxCount) {
        return new CountTriggerDebug<W>(maxCount);
    }

    /**
     * 定义一个 Reducing 状态的 reducing函数，处理过程如下
     */
    private static class SUM implements ReduceFunction<Long> {
        @Override
        public Long reduce(Long value1, Long value2) throws Exception {
            System.out.printf("触发器调用 reduce 方法 %s , %s %n", value1, value2);
            return value1 + value2;
        }
    }
}
