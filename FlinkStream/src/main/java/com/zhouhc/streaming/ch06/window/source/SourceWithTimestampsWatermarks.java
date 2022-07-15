package com.zhouhc.streaming.ch06.window.source;

import com.zhouhc.streaming.ch06.window.time.bean.EventBean;
import com.zhouhc.streaming.ch06.window.util.TimeUtils;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.sql.Time;

/**
 * 带有水印和时间戳的数据
 */
public class SourceWithTimestampsWatermarks implements SourceFunction<EventBean> {
    private static final long serialVersionUID = 1L;

    private volatile boolean isRunning = true;

    private int counter = 0;

    private long sleepTime;

    public SourceWithTimestampsWatermarks(long sleepTime) {
        this.sleepTime = sleepTime;
    }

    @Override
    public void run(SourceContext<EventBean> ctx) throws Exception {
        while (isRunning) {
            if (counter >= 16) {
                isRunning = false;
            } else {
                EventBean eventBean = Data.BEANS[counter];
                ctx.collectWithTimestamp(eventBean, eventBean.getTime());
                System.out.printf("send 元素内容 [%s] now time : %s%n", eventBean, TimeUtils.getHHmmss(eventBean.getTime()));
                //手动发送水印
                if (eventBean.getList().get(0).indexOf("late") < 0)
                    ctx.emitWatermark(new Watermark(System.currentTimeMillis()));
                //是否需要休眠一段时间
                if (eventBean.getList().get(0).indexOf("nosleep") < 0)
                    Thread.sleep(sleepTime);
            }
            counter++;
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
