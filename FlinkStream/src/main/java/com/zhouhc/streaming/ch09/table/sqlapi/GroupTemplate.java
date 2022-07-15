package com.zhouhc.streaming.ch09.table.sqlapi;

import com.zhouhc.streaming.ch09.table.bean.ClickBean;
import com.zhouhc.streaming.ch09.table.poData.PrepareData;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 使用 sql 操作的聚合对象
 */
public class GroupTemplate {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
        //添加数据源
        DataStream<ClickBean> clickBeanDataStreamSource = environment.fromCollection(PrepareData.getClicksData());
        //注册表
        tableEnvironment.createTemporaryView("Clicks",clickBeanDataStreamSource);
        //执行sql语句
        Table resultTable = tableEnvironment.sqlQuery("select user,count(url) from Clicks group by user");
        tableEnvironment.toChangelogStream(resultTable).print();
        environment.execute("GroupTemplate");
    }
}
