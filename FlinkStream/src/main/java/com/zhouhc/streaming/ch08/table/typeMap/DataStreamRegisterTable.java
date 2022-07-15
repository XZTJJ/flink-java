package com.zhouhc.streaming.ch08.table.typeMap;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Arrays;

/**
 *  按照 table 的方式操作 stream ,其实也是就是
 *  将 datastream 转成 table 的操作
 */
public class DataStreamRegisterTable {

    public static void main(String[] args) throws Exception {
        //更具流的 datastream 创建 table 的执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(2);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
        //创建 dataStream 的 source
        DataStream<Tuple2<Long, String>> tuple2DataStreamSource = environment.fromCollection(Arrays.asList(
                new Tuple2<Long, String>(1L, "手机"),
                new Tuple2<Long, String>(2L, "平板"),
                new Tuple2<Long, String>(3L, "电脑")
        ));
        //转成table
        tableEnvironment.createTemporaryView("table_order",tuple2DataStreamSource);
        //执行操作
        Table table = tableEnvironment.sqlQuery("select * from table_order where f0 < 3 ");
        tableEnvironment.toDataStream(table).print("sql");
        //转换结果
        environment.execute("DataStreamRegisterTable");
    }
}
