package com.zhouhc.streaming.ch06.window.time;

import com.zhouhc.streaming.ch06.window.source.SourceWithTimestamps;
import com.zhouhc.streaming.ch06.window.source.SourceWithTimestampsWatermarks;
import com.zhouhc.streaming.ch06.window.time.bean.EventBean;
import com.zhouhc.streaming.ch06.window.util.TimeUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;


import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Date;

/**
 * 在操作符中分配时间和水印
 */
public class EventTimeTemplate {

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
        environment.getConfig().setAutoWatermarkInterval(5000);
        //决定是否延迟输出数据
        String flag = "允许延迟";
        if (flag.equals("不允许延迟")) {
            //定义输入源,并且并且设置水印和时间时间字段
            DataStream<EventBean> sourceStream = environment.addSource(new SourceWithTimestamps(10000))
                    .assignTimestampsAndWatermarks(getCurrentWatermarkStrategy());
            DataStream<EventBean> reduceStream = sourceStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(30)))
                    .reduce(new ReduceFunction<EventBean>() {
                        @Override
                        public EventBean reduce(EventBean value1, EventBean value2) throws Exception {
                            value1.getList().addAll(value2.getList());
                            return value1;
                        }
                    });
            reduceStream.print("不允许延迟");
        } else if (flag.equals("允许延迟")) {
            //定义输入源,并且并且设置水印和时间时间字段
            DataStream<EventBean> sourceStream1 = environment.addSource(new SourceWithTimestampsWatermarks(10000))
                    .assignTimestampsAndWatermarks(getCurrentWatermarkStrategy());
            //设置延迟结果
            DataStream<EventBean> reduceStream = sourceStream1.windowAll(TumblingEventTimeWindows.of(Time.seconds(30)))
                    .allowedLateness(Time.seconds(15))
                    .reduce(new ReduceFunction<EventBean>() {
                        @Override
                        public EventBean reduce(EventBean value1, EventBean value2) throws Exception {
                            value1.getList().addAll(value2.getList());
                            return value1;
                        }
                    });
            reduceStream.print("允许延迟15s");
        } else if (flag.equals("输出延迟元素")) {
            //定义输入源,并且并且设置水印和时间时间字段
            DataStream<EventBean> sourceStream = environment.addSource(new SourceWithTimestamps(10000))
                    .assignTimestampsAndWatermarks(getCurrentWatermarkStrategy());
            //设置延迟结果
            SingleOutputStreamOperator<EventBean> reduceStream = sourceStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(30)))
                    .allowedLateness(Time.seconds(15))
                    .sideOutputLateData(new OutputTag<EventBean>("lateElement", TypeInformation.of(new TypeHint<EventBean>() {})))
                    .reduce(new ReduceFunction<EventBean>() {
                        @Override
                        public EventBean reduce(EventBean value1, EventBean value2) throws Exception {
                            value1.getList().addAll(value2.getList());
                            return value1;
                        }
                    });

            reduceStream.getSideOutput(new OutputTag<EventBean>("lateElement", TypeInformation.of(new TypeHint<EventBean>() {})))
                    .print("late elements is  --->>>");
            reduceStream.print("输出延迟元素");
        }
        environment.execute("EventTimeTemplate");
    }


    //调用水印盛生成策略，并且设置事件时间字段
    private static WatermarkStrategy<EventBean> getCurrentWatermarkStrategy() {
        return WatermarkStrategy.<EventBean>forBoundedOutOfOrderness(Duration.ofSeconds(0)).withTimestampAssigner(new SerializableTimestampAssigner<EventBean>() {
            @Override
            public long extractTimestamp(EventBean element, long recordTimestamp) {
                return element.getTime();
            }
        });
    }


}
