package com.zhouhc.streaming.ch08.table.typeMap;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.expressions.In;
import org.apache.flink.types.Row;

import java.util.Arrays;

/**
 * 将 table 转成 stream 来处理各种数据
 */
public class TableConvertDataStream {
    public static void main(String[] args) throws Exception {
        //更具流的 datastream 创建 table 的执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(2);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
        //创建 datastream 的source
        DataStream<Tuple3<Long, String, Integer>> tuple2DataStreamSource = environment.fromCollection(Arrays.asList(
                new Tuple3<Long, String, Integer>(1L, "手机", 1899),
                new Tuple3<Long, String, Integer>(2L, "平板", 8888),
                new Tuple3<Long, String, Integer>(3L, "电脑", 899)
        ));
        //指定字段
        Schema tableSchema = Schema.newBuilder().column("f0", DataTypes.BIGINT()).column("f1", DataTypes.STRING())
                .column("f2", DataTypes.INT()).build();

        //注册表，同时指定字段，只能使用这种方式
        Table customTable = tableEnvironment.fromDataStream(tuple2DataStreamSource, tableSchema).as("user", "product", "amount");
        tableEnvironment.createTemporaryView("table_order", customTable);
        //如果使用这种方式的话，必须要字段名和DataStream中的字段名一致
        //tableEnvironment.createTemporaryView("table_order",tuple2DataStreamSource,tableSchema);
        //指定查询语句
        Table rowResult = tableEnvironment.sqlQuery("select user,product,amount from table_order where amount < 3000");
        //转换成 DataStream的Row类型
        DataStream<Row> rowDataStream = tableEnvironment.toDataStream(rowResult);
        rowDataStream.print("Row Type : ");

        //转成单字段类型
        //指定查询语句
        Table atomicResult = tableEnvironment.sqlQuery("select amount from table_order where amount < 3000");
        //转换成 DataStream的Row类型
        DataStream<Integer> atomicDataStream = tableEnvironment.toDataStream(atomicResult, Integer.class);
        atomicDataStream.print("atomic Type : ");

        //转成多字段类型
        //指定查询语句
        Table tupleResult = tableEnvironment.sqlQuery("select user,amount from table_order where amount < 3000");
        //转换成 DataStream的tuple类型
        DataStream<Tuple2<Long, Integer>> objectDataStream = tableEnvironment.toDataStream(
                tupleResult, DataTypes.of(TypeInformation.of(new TypeHint<Tuple2<Long, Integer>>() {
                })));
        objectDataStream.print("tuple Type : ");

        //转成pojo类型
        //指定查询语句
        Table pojoResult = tableEnvironment.sqlQuery("select user,product,amount from table_order where amount < 3000");
        //转换成 DataStream的tuple类型
        DataStream<OrderBean> pojoDataStream = tableEnvironment.toDataStream(pojoResult, DataTypes.of(OrderBean.class));
        pojoDataStream.print("pojo Type : ");
        //设置任务执行
        environment.execute("TableConvertDataStream");
    }

    //i个单独的pojo数据类型
    public static class OrderBean {
        private long user;
        private String product;
        private int amount;

        public OrderBean() {
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
            return String.format("OrderBean(%s,%s,%s)", user, product, amount);
        }
    }
}
