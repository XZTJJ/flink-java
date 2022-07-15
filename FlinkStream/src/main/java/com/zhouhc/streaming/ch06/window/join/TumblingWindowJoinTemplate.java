package com.zhouhc.streaming.ch06.window.join;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.JoinedStreams;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;

/**
 * 滚动窗口的 join 操作
 */
public class TumblingWindowJoinTemplate {

    public static void main(String[] args) throws Exception {
        //执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //两个固定的数据源,并且设置好 水印和时间字段
        List<PrepareData.ClickBean> clicksData = PrepareData.getClicksData();
        DataStream<PrepareData.ClickBean> clickBeanDataStreamSource = environment.fromCollection(clicksData)
                .assignTimestampsAndWatermarks(getClickBeanWatermark());
        List<PrepareData.Trade> tradeData = PrepareData.getTradeData();
        DataStream<PrepareData.Trade> tradeDataStreamSource = environment.fromCollection(tradeData)
                .assignTimestampsAndWatermarks(getTradeBeanWatermark());
        //两个窗口进行join操作
        JoinedStreams.WithWindow<PrepareData.ClickBean, PrepareData.Trade, String, TimeWindow> window =
                clickBeanDataStreamSource.join(tradeDataStreamSource)
                        .where((KeySelector<PrepareData.ClickBean, String>) value -> value.getUser(),
                                TypeInformation.of(new TypeHint<String>() {
                                }))
                        .equalTo((KeySelector<PrepareData.Trade, String>) value -> value.getName(),
                                TypeInformation.of(new TypeHint<String>() {
                                }))
                        .window(TumblingEventTimeWindows.of(Time.hours(1L)));
        //对两个流进行，普通的join操作
        DataStream<Object> applyStream = window.apply(new JoinFunction<PrepareData.ClickBean, PrepareData.Trade, Object>() {
            @Override
            public Object join(PrepareData.ClickBean first, PrepareData.Trade second) throws Exception {
                return first + " : " + second;
            }
        });
        //对两个流进行, flatJoin操作
        DataStream<String> flatApplyStream = window.apply(new FlatJoinFunction<PrepareData.ClickBean, PrepareData.Trade, String>() {
            @Override
            public void join(PrepareData.ClickBean first, PrepareData.Trade second, Collector<String> out) throws Exception {
                out.collect(first.getUser() + " : " + first.getVisitTime() + " : " + second.getTradeTime());
                out.collect(first.getUser() + " : " + first.getUrl() + " : " + second.getClient());
            }
        });
        applyStream.print("applyStream");
        flatApplyStream.print("flatApplyStream");
        environment.execute("TumblingWindowJoinTemplate");
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
