package com.zhouhc.streaming.ch09.table.sqlapi;

import com.zhouhc.streaming.ch09.table.bean.ClickBean;
import com.zhouhc.streaming.ch09.table.poData.PrepareData;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 对窗口进行分组操作
 */
public class GroupWindowTemplate {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
        //添加数据源
        DataStream<ClickBean> clickBeanDataStreamSource = environment.fromCollection(PrepareData.getClicksData())
                .assignTimestampsAndWatermarks(getClickBeanWaterMark());
        //注册Schema
//        Schema schema = Schema.newBuilder()
//                .column("user", DataTypes.STRING())
//                .column("time", DataTypes.TIMESTAMP())
//                .column("url", DataTypes.STRING())
//                .column("id", DataTypes.INT())
//                .columnByMetadata("rowtime",DataTypes.TIMESTAMP(3))
//                .build();
        //注册表
        Table registerTable = tableEnvironment.fromDataStream(clickBeanDataStreamSource,
                $("user"),$("time").as("etime"),$("url"),$("id"),$("rowtime").rowtime());
        tableEnvironment.createTemporaryView("Clicks", registerTable);
        //执行sql语句
        Table resultTable = tableEnvironment.sqlQuery("select user,count(url),TUMBLE_START(rowtime, INTERVAL '1' HOUR)" +
                ",TUMBLE_ROWTIME(rowtime, INTERVAL '1' HOUR), TUMBLE_END(rowtime, INTERVAL '1' HOUR) from Clicks " +
                " group by TUMBLE(rowtime, INTERVAL '1' HOUR),user");
        tableEnvironment.toDataStream(resultTable).print();
        environment.execute("GroupWindowTemplate");
    }

    //没有延迟数据
    private static WatermarkStrategy<ClickBean> getClickBeanWaterMark() {
        return WatermarkStrategy.<ClickBean>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                .withTimestampAssigner((element, recordTimestamp) -> element.getTime().getTime());
    }
}
