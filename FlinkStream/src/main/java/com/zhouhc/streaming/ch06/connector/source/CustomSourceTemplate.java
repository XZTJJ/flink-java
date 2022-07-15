package com.zhouhc.streaming.ch06.connector.source;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

//自定义的并行度为1的数据源
public class CustomSourceTemplate implements SourceFunction<Long> {
    //控制是否停止运行的状态
    private volatile boolean isRunning = true;
    //计数器
    private Long count = 0L;
    //休眠时间
    private final long sleepTime;

    public CustomSourceTemplate(long sleepTime) {
        this.sleepTime = sleepTime;
    }

    @Override
    public void run(SourceContext<Long> ctx) throws Exception {
        while (isRunning) {
            ctx.collect(count);
            //发送元素
            System.out.printf("send data is %s %n", count);
            count++;
            Thread.sleep(sleepTime);
        }
    }

    @Override
    public void cancel() {
        System.out.printf("停止发送元素%n");
        isRunning = false;
    }


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.addSource(new CustomSourceTemplate(100)).map(value -> value + System.currentTimeMillis())
                .print("CustomSourceTemplate");
        env.execute("CustomSourceTemplate");
    }
}
