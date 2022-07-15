package com.zhouhc.streaming.ch04.taskchainslot;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

/**
 * 自定义数据源
 */
public class ChainSource extends RichSourceFunction<Tuple2<String, Long>> {
    //
    private static final long serialVersionUID = 1L;

    private final int sleepTime = 3000;

    @Override
    public void run(SourceContext<Tuple2<String, Long>> ctx) throws Exception {
        String subtaskName = getRuntimeContext().getTaskNameWithSubtasks();
        String info = "source操作所属的子任务名称";
        Tuple2<String, Long> tuple2 = new Tuple2<String, Long>("185XXX", 899L);
        ctx.collect(tuple2);
        System.out.printf("%s : %s , 元素 : %s %n", info, subtaskName, tuple2);
        Thread.sleep(sleepTime);

        tuple2 = new Tuple2<String, Long>("155XXX", 1199L);
        ctx.collect(tuple2);
        System.out.printf("%s : %s , 元素 : %s %n", info, subtaskName, tuple2);
        Thread.sleep(sleepTime);


        tuple2 = new Tuple2<String, Long>("138XXX", 19L);
        ctx.collect(tuple2);
        System.out.printf("%s : %s , 元素 : %s %n", info, subtaskName, tuple2);
        Thread.sleep(sleepTime);
    }

    @Override
    public void cancel() {

    }
}
