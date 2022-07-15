package com.zhouhc.streaming.ch06.window.join;


import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;

/**
 * 间隔 join , 不使用 window的使用
 * 直接使用 事件时间的 方式, 目前只是
 * 支持事件时间
 */
public class IntervalJoinTemplate {

    public static void main(String[] args) throws Exception {
        //环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置数据源，并且 keyedBy 操作
        List<PrepareData.ClickBean> clicksData = PrepareData.getClicksData();
        KeyedStream<PrepareData.ClickBean, String> clickBeanStringKeyedStream = environment.fromCollection(clicksData)
                .assignTimestampsAndWatermarks(getClickBeanWatermark())
                .keyBy((KeySelector<PrepareData.ClickBean, String>) value -> value.getUser());
        //同上
        List<PrepareData.Trade> tradeData = PrepareData.getTradeData();
        KeyedStream<PrepareData.Trade, String> tradeStringKeyedStream = environment.fromCollection(tradeData)
                .assignTimestampsAndWatermarks(getTradeBeanWatermark())
                .keyBy((KeySelector<PrepareData.Trade, String>) value -> value.getName());
        //两条 keyed 流进行相应的处理
        SingleOutputStreamOperator<String> processStream = clickBeanStringKeyedStream.intervalJoin(tradeStringKeyedStream)
                //设置两条流的上下限时间
                .between(Time.minutes(-30), Time.minutes(20))
                //设置处理过程
                .process(new ProcessJoinFunction<PrepareData.ClickBean, PrepareData.Trade, String>() {
                    @Override
                    public void processElement(PrepareData.ClickBean left,
                                               PrepareData.Trade right,
                                               ProcessJoinFunction<PrepareData.ClickBean, PrepareData.Trade, String>.Context ctx,
                                               Collector<String> out) throws Exception {
                        out.collect(left.toString() + " : " + right.toString());
                    }
                });
        processStream.print("IntervalJoinTemplate");
        environment.execute("IntervalJoinTemplate");
    }

    //定义ClickBean的水印策略
    private static WatermarkStrategy<PrepareData.ClickBean> getClickBeanWatermark() {
        return WatermarkStrategy.<PrepareData.ClickBean>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                .withTimestampAssigner(new SerializableTimestampAssigner<PrepareData.ClickBean>() {
                    @Override
                    public long extractTimestamp(PrepareData.ClickBean element, long recordTimestamp) {
                        return element.getVisitTime().getTime();
                    }
                });
    }

    //定义TradeBean的水印策略
    private static WatermarkStrategy<PrepareData.Trade> getTradeBeanWatermark() {
        return WatermarkStrategy.<PrepareData.Trade>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                .withTimestampAssigner(new SerializableTimestampAssigner<PrepareData.Trade>() {
                    @Override
                    public long extractTimestamp(PrepareData.Trade element, long recordTimestamp) {
                        return element.getTradeTime().getTime();
                    }
                });
    }
}
