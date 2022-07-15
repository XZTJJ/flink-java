package com.zhouhc.streaming.ch05.operatorstate;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

/**
 * 自定义数据源
 */
public class CustomSource extends RichSourceFunction<Long> {

    private static final long serialVersionUID = 1L;

    @Override
    public void run(SourceContext<Long> ctx) throws Exception {
        long offSet = 0L;
        while (true) {
            System.out.printf("%s 发送数据 : %s%n", Thread.currentThread().getName(), offSet);
            ctx.collect(offSet);
            offSet = offSet + 1;
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {

    }
}
