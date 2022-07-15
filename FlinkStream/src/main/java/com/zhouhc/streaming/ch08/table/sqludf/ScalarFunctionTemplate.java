package com.zhouhc.streaming.ch08.table.sqludf;

import jdk.nashorn.internal.runtime.ParserException;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.List;

/**
 * 自定义的 scalar 函数，该函数只会返回一个对象
 * 必须继承对应的  ScalarFunction 函数
 */
public class ScalarFunctionTemplate extends ScalarFunction {
    //实现计算方法,指定格式返回
    public String eval(String dateStr) throws ParserException {
        DateTimeFormatter sourceFormater = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        DateTimeFormatter targetFormater = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        TemporalAccessor sourceDate = sourceFormater.parse(dateStr);
        return targetFormater.format(sourceDate);
    }

    //实现计算逻辑,和上面一样，只不过日期，提前了指定的天数
    public String eval(String dateStr, int num) throws ParserException {
        DateTimeFormatter sourceFormater = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        DateTimeFormatter targetFormater = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        TemporalAccessor sourceDate = sourceFormater.parse(dateStr);
        LocalDateTime targetDateTime = LocalDateTime.from(sourceDate).minusDays(num);
        return targetDateTime.format(targetFormater);
    }

    //对应的方法
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(2);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
        //添加数据源
        List<Tuple3<String, String, String>> sourceList = new ArrayList<Tuple3<String, String, String>>();
        sourceList.add(Tuple3.of("intsmaze", "2019-07-28 12:00:00", ".../intsmaze/"));
        sourceList.add(Tuple3.of("Flink", "2019-07-25 12:00:00", ".../intsmaze/"));
        DataStreamSource<Tuple3<String, String, String>> dataStreamSource = environment.fromCollection(sourceList);
        //注册成表
        Table orderRegister = tableEnvironment.fromDataStream(dataStreamSource).as("user", "visit_time", "url");
        tableEnvironment.createTemporaryView("testSUDF", orderRegister);
        //注册自定义的 scalar UDF 函数
        tableEnvironment.createTemporaryFunction("custom_Date", new ScalarFunctionTemplate());
        //执行sql语句
        Table customTable = tableEnvironment.sqlQuery("select user,custom_Date(visit_time),url,custom_Date(visit_time,2) from testSUDF");
        tableEnvironment.toDataStream(customTable).print("result:");
        //指定执行方法
        environment.execute("ScalarFunctionTemplate");
    }
}
