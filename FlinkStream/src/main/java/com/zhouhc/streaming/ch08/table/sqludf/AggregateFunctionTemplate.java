package com.zhouhc.streaming.ch08.table.sqludf;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;

/**
 * 自定义的 聚合函数，与标量函数不同的是，
 * 该函数为定义的聚合操作，需要配合 group by
 * 来使用
 */
public class AggregateFunctionTemplate extends AggregateFunction<Double, AggregateFunctionTemplate.TempAccountAvg> {
    //常用聚合操作
    @Override
    public Double getValue(TempAccountAvg accumulator) {
        return accumulator.totalPrice / accumulator.totalNum;
    }

    @Override
    public TempAccountAvg createAccumulator() {
        return new TempAccountAvg();
    }

    public void accumulate(TempAccountAvg accumulator, Double pricer, Integer num) {
        accumulator.totalPrice += pricer;
        accumulator.totalNum += num;
    }


    //对应的方法
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(2);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
        //添加数据源
        List<Row> sourceList = new ArrayList<Row>();
        sourceList.add(Row.of("张三", "可乐", 20.0D, 4));
        sourceList.add(Row.of("张三", "果汁", 10.0D, 4));
        sourceList.add(Row.of("李四", "咖啡", 10.0D, 2));
        DataStream<Row> dataStreamSource = environment.fromCollection(sourceList).returns(Types.ROW(
                TypeInformation.of(new TypeHint<String>() {}),
                TypeInformation.of(new TypeHint<String>() {}),
                TypeInformation.of(new TypeHint<Double>() {}),
                TypeInformation.of(new TypeHint<Integer>() {})
        ));
        //注册成表
        Table orderRegister = tableEnvironment.fromDataStream(dataStreamSource).as("user", "name", "price", "num");
        tableEnvironment.createTemporaryView("testAUDF", orderRegister);
        //注册自定义的 scalar UDF 函数
        tableEnvironment.createTemporaryFunction("aggFunction", new AggregateFunctionTemplate());
        //执行sql语句
        Table aggTable = tableEnvironment.sqlQuery("select user,aggFunction(price,num) from testAUDF group by user");
        tableEnvironment.toChangelogStream(aggTable).print("agg:");
        //指定执行方法
        environment.execute("AggregateFunctionTemplate");
    }

    //自定义函数
    public static class TempAccountAvg {
        private double totalPrice;
        private int totalNum;

        public TempAccountAvg() {
        }

        public double getTotalPrice() {
            return totalPrice;
        }

        public void setTotalPrice(double totalPrice) {
            this.totalPrice = totalPrice;
        }

        public int getTotalNum() {
            return totalNum;
        }

        public void setTotalNum(int totalNum) {
            this.totalNum = totalNum;
        }
    }
}
