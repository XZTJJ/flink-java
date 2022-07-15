package com.zhouhc.streaming.ch06.window.time;

import com.zhouhc.streaming.ch06.window.source.SourceWithTimestampsWatermarks;
import com.zhouhc.streaming.ch06.window.time.bean.EventBean;
import com.zhouhc.streaming.ch06.window.util.TimeUtils;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.LocalDateTime;
import java.util.Date;

/**
 * 在source端就已经进行 事件时间 的分配了。
 */
public class EventTimeSourceTemplate {

    public static void main(String[] args) throws Exception {
        //设置程序开始时间,使得结果一致
        while (true) {
            int offset = LocalDateTime.now().getSecond();
            if (offset <= 0 || (offset >= 30 && offset <= 35))
                break;
        }
        System.out.printf("start time %s %n", TimeUtils.getHHmmss(new Date()));
        //设置时间戳,相关信息
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        //environment.getConfig().setAutoWatermarkInterval(5000);
        DataStream<EventBean> sourceStream = environment.addSource(new SourceWithTimestampsWatermarks(10000));
        DataStream<EventBean> reduceStream = sourceStream
                //定义一个基于事件时间的滚动窗口，窗口大小为30s
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(30)))
                //在窗口中应用 reduceFunction 窗口函数
                .reduce(new ReduceFunction<EventBean>() {
                    @Override
                    public EventBean reduce(EventBean value1, EventBean value2) throws Exception {
                        value1.getList().addAll(value2.getList());
                        return value1;
                    }
                });
        //输出结果
        reduceStream.print("EventTimeSourceTemplate");
        environment.execute("EventTimeSourceTemplate");
    }
}
