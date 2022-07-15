package com.zhouhc.streaming.ch08.table.sqludf;

import cn.hutool.core.util.StrUtil;
import jdk.nashorn.internal.runtime.ParserException;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * 自定义的 表函数，与标量函数不同的是，
 * 该函数可以返回任意数量的结果
 */
public class TableFunctionTemplate extends TableFunction<Tuple2<String, Integer>> {
    private String separator;

    //指定字段类型
    public TableFunctionTemplate(String separator) {
        this.separator = separator;
    }

    //实现计算方法,指定格式返回
    public void eval(String words) throws ParserException {
        if (StrUtil.indexOf(words, "flink", 0, false) < 0) {
            for(String word : StrUtil.split(words,separator,-1,true,true)){
                collect(Tuple2.of(word,word.length()));
            }
        }
    }


    //对应的方法
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(2);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
        //添加数据源
        List<OrderBeanInnder> sourceList = new ArrayList<OrderBeanInnder>();
        sourceList.add(new OrderBeanInnder(1L, "beer#intsmaze", 3));
        sourceList.add(new OrderBeanInnder(1L, "flink#intsmaze", 4));
        sourceList.add(new OrderBeanInnder(3L, "rubber#intsmaze", 2));
        DataStreamSource<OrderBeanInnder> dataStreamSource = environment.fromCollection(sourceList);
        //注册成表
        Table orderRegister = tableEnvironment.fromDataStream(dataStreamSource);
        tableEnvironment.createTemporaryView("testTUDF", orderRegister);
        //注册自定义的 scalar UDF 函数
        tableEnvironment.createTemporaryFunction("splitFunction", new TableFunctionTemplate("#"));
        //执行sql语句
        Table crossJoinTable = tableEnvironment.sqlQuery("select user,product,amount,word,length from testTUDF,LATERAL TABLE(splitFunction(product)) as T(word,length)");
        tableEnvironment.toDataStream(crossJoinTable).print("crossJoinTable:");
        //另一个连接的 sql 语句
        Table leftJoinTable = tableEnvironment.sqlQuery("select user,product,amount,word,length from testTUDF " +
                "left join LATERAL TABLE(splitFunction(product)) as T(word,length) on true");
        tableEnvironment.toDataStream(leftJoinTable).print("leftJoinTable:");
        //指定执行方法
        environment.execute("TableFunctionTemplate");
    }



    public static class OrderBeanInnder{
        private long user;
        private String product;
        private int amount;

        public OrderBeanInnder() {
        }

        public OrderBeanInnder(long user, String product, int amount) {
            this.user = user;
            this.product = product;
            this.amount = amount;
        }

        public long getUser() {
            return user;
        }

        public void setUser(long user) {
            this.user = user;
        }

        public String getProduct() {
            return product;
        }

        public void setProduct(String product) {
            this.product = product;
        }

        public int getAmount() {
            return amount;
        }

        public void setAmount(int amount) {
            this.amount = amount;
        }

        @Override
        public String toString() {
            return "OrderBeanInnder{" +
                    "user=" + user +
                    ", product='" + product + '\'' +
                    ", amount=" + amount +
                    '}';
        }
    }
}
