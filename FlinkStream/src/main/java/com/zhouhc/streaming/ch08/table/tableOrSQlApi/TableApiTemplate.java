package com.zhouhc.streaming.ch08.table.tableOrSQlApi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;


/**
 * 使用 table / sql 的方式对 文件系统的 csv 文件进行读取
 * 并且打印到 控制台
 */
public class TableApiTemplate {

    public static void main(String[] args) throws Exception {
        //更具流的 datastream 创建 table 的执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(2);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
        //创建 输入表的 schema 格式
        Schema tableSchema = Schema.newBuilder()
                .column("name", DataTypes.STRING())
                .column("age", DataTypes.INT())
                .column("city", DataTypes.STRING())
                .build();
        //指定文件格式
        FormatDescriptor csvFormat = FormatDescriptor.forFormat("csv").option("field-delimiter", ",").build();
        //文件路径
        String FilePath = "file:///F:/logs/tablesource/TemplateCsvFile.txt";
        //更具描述符创建一个csv文件
        TableDescriptor csvSourceDescriptor = TableDescriptor.forConnector("filesystem")
                .schema(tableSchema)
                .option("path", FilePath)
                .format(csvFormat)
                .build();
        //创建一个临时表/视图, 使用一个 默认的 catalog 和 database
        tableEnvironment.createTemporaryTable("Person",csvSourceDescriptor);
        //使用 table api 进行操作，而不是 sql 操作
        Table result = tableEnvironment.from("Person").filter($("age").isGreater(30)).groupBy($("name"))
                .select($("name"), $("name").count());
        //输出类型可以不用写的
        Schema sinkSchema = Schema.newBuilder()
                .column("name", DataTypes.STRING())
                .column("count", DataTypes.BIGINT())
                .build();
        //输出到控制台，不需要输出表
        tableEnvironment.toChangelogStream(result).print();
        //触发执行程序
        environment.execute("TableApiTemplate");
    }
}
