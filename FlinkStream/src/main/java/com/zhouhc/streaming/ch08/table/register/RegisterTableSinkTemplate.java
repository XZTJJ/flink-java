package com.zhouhc.streaming.ch08.table.register;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * 和 RegisterTableTemplate 功能类似，只不过
 * 是将结果 sink 到一张表中，表连接的时本地的文件
 * 系统，格式为csv文件
 */
public class RegisterTableSinkTemplate {
    public static void main(String[] args) throws Exception {
        //更具流的 datastream 创建 table 的执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(2);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
        //创建 输入表的 schema 格式
        Schema tableSourceSchema = Schema.newBuilder()
                .column("name", DataTypes.STRING())
                .column("age", DataTypes.INT())
                .column("city", DataTypes.STRING())
                .build();
        //指定文件格式
        FormatDescriptor csvSourceFormat = FormatDescriptor.forFormat("csv").option("field-delimiter", ",").build();
        //文件路径
        String sourceFilePath = "file:///F:/logs/tablesource/TemplateCsvFile.txt";
        //更具描述符创建一个csv文件
        TableDescriptor csvSourceDescriptor = TableDescriptor.forConnector("filesystem")
                .schema(tableSourceSchema)
                .option("path", sourceFilePath)
                .format(csvSourceFormat)
                .build();
        //创建一个临时表/视图,作为source, 使用一个 默认的 catalog 和 database
        tableEnvironment.createTemporaryTable("Person", csvSourceDescriptor);

        //创建一个输出表，输出表为文件系统，并且格式为 csv 文件
        //创建 输入表的 schema 格式
        Schema tableSinkSchema = Schema.newBuilder()
                .column("name", DataTypes.STRING())
                .column("age", DataTypes.INT())
                .column("city", DataTypes.STRING())
                .build();
        //指定文件格式
        FormatDescriptor csvSinkFormat = FormatDescriptor.forFormat("csv").option("field-delimiter", "|").build();
        //文件路径
        String sinkFilePath = "file:///F:/logs/tablesink/csv";
        //更具描述符创建一个csv文件
        TableDescriptor csvSinkDescriptor = TableDescriptor.forConnector("filesystem")
                .schema(tableSinkSchema)
                .option("path", sinkFilePath)
                .format(csvSinkFormat)
                .build();
        //创建一个临时表/视图,作为sink, 使用一个 默认的 catalog 和 database
        tableEnvironment.createTemporaryTable("CsvSinkTable", csvSinkDescriptor);
        //执行语句,并且将结果输出
        Table result = tableEnvironment.sqlQuery("select name,age,city from Person where age > 30");
        //只有一个sink的执行方式
        result.executeInsert("CsvSinkTable");
        //多个sink的执行方式
//        StatementSet stmtSet = tableEnvironment.createStatementSet();
//        stmtSet.addInsert(csvSinkDescriptor, result).execute();

    }
}
