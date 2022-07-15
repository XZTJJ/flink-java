package com.zhouhc.streaming.ch08.table.register;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 *  和  CsvSourceTemplate 类似，不过这里面的结果先输出到
 *  一张新的注册表中，并且将新表的结果输出
 */
public class RegisterTableTemplate {
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
        //创建一个执行语句
        Table result = tableEnvironment.sqlQuery("select name,age,city from Person where age > 30");
        //结果输入到新表中，并且并且查询结果输出
        tableEnvironment.createTemporaryView("resultTable",result);
        Table resultTwo = tableEnvironment.sqlQuery("select name,age,city from resultTable");
        //输出到控制台，不需要输出表
        tableEnvironment.toDataStream(resultTwo, Row.class).print();
        //触发执行程序
        environment.execute("RegisterTableTemplate");
    }
}
