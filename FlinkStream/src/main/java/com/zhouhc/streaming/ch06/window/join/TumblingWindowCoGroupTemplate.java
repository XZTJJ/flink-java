package com.zhouhc.streaming.ch06.window.join;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.CoGroupedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;

/**
 * 滚动窗口的 coGroup 操作, 和join操作不同的是
 * coGroup允许输出一个流为空的情况
 */
public class TumblingWindowCoGroupTemplate {

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
        CoGroupedStreams.WithWindow<PrepareData.ClickBean, PrepareData.Trade, String, TimeWindow> window =
                clickBeanDataStreamSource.coGroup(tradeDataStreamSource)
                        .where((KeySelector<PrepareData.ClickBean, String>) value -> value.getUser(),
                                TypeInformation.of(new TypeHint<String>() {
                                }))
                        .equalTo((KeySelector<PrepareData.Trade, String>) value -> value.getName(),
                                TypeInformation.of(new TypeHint<String>() {
                                }))
                        .window(TumblingEventTimeWindows.of(Time.hours(1L)));
        //对两个流进行，普通的join操作
        DataStream<String> applyStream = window.apply(new CoGroupFunction<PrepareData.ClickBean, PrepareData.Trade, String>() {
            @Override
            public void coGroup(Iterable<PrepareData.ClickBean> first, Iterable<PrepareData.Trade> second, Collector<String> out) throws Exception {
                //开始进行遍历
                for (PrepareData.ClickBean clickBean : first) {
                    String mess = "";
                    for (PrepareData.Trade trade : second) {
                        mess = mess + "--" + trade.getName() + "--" + trade.getClient();
                    }
                    out.collect(clickBean.getUser() + ":" + clickBean.getUrl() + ":" + mess);
                }
            }
        });
        applyStream.print("applyStream");
        environment.execute("TumblingWindowCoGroupTemplate");
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
