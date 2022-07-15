package com.zhouhc.streaming.ch05.keystate;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 简单的单值统计信息,不过携带过期时间了
 */
public class TTLValueStateFlatMap {
    private final static Logger LOGGER = LoggerFactory.getLogger(TTLValueStateFlatMap.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        KeyedStream<Tuple2<Integer, Integer>, Integer> keystream = KeyStateBase.before(environment);
        DataStream<Tuple2<Integer, Integer>> flatMapStream = keystream.flatMap(new MyRichFlatMap(true,false));
        flatMapStream.print("TTLValueStateFlatMap");
        environment.execute("TTLValueStateFlatMap");
    }


    /**
     * 设置过滤函数
     */
    private static class MyRichFlatMap extends RichFlatMapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {
        //创建状态句柄
        private transient ValueState<Tuple2<Integer, Integer>> valueState;
        //显示是否为读写更新生存周期
        private boolean isRead;
        //元素过期是否返回
        private boolean isReturn;

        public MyRichFlatMap(boolean isRead, boolean isReturn) {
            this.isRead = isRead;
            this.isReturn = isReturn;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            LOGGER.info("{} 开始初始化状态", Thread.currentThread().getName());
            //创建文件描述符
            ValueStateDescriptor<Tuple2<Integer, Integer>> stateDescriptor = new ValueStateDescriptor<Tuple2<Integer, Integer>>("uniqueName",
                    TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>() {
                    }));
            //设置对应的过期时间，过期时间为6s
            StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.seconds(6))
                    //设置更新最后ttl的方式，是读写更新，还是写更新
                    .setUpdateType(isRead ? StateTtlConfig.UpdateType.OnReadAndWrite : StateTtlConfig.UpdateType.OnCreateAndWrite)
                    //设置是否过期返回
                    .setStateVisibility(isReturn ? StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp : StateTtlConfig.StateVisibility.NeverReturnExpired).build();
            //设置过期配置
            stateDescriptor.enableTimeToLive(ttlConfig);
            //初始化状态
            valueState = getRuntimeContext().getState(stateDescriptor);
        }

        @Override
        public void flatMap(Tuple2<Integer, Integer> value, Collector<Tuple2<Integer, Integer>> out) throws Exception {
            //决定写入的时间
            if (value.f1 > 10 && value.f1 < 20) {
                //直接返回吧
                if (isRead) {
                    out.collect(value);
                } else {
                    Tuple2<Integer, Integer> temp = valueState.value();
                    System.out.printf("f1 字段符合 10 ~ 20之间 , 不执行任何操作 , key 为 %s 的状态为 : %s %n", value.f0, temp);
                }
            } else {
                Tuple2<Integer, Integer> temp = valueState.value();
                System.out.printf("f1 字段不在 10 ~ 20之间 , 不执行任何操作 , key 为 %s 的状态为 : %s %n", value.f0, temp);
                if (temp == null)
                    temp = value;
                else
                    temp.f1 = temp.f1 + value.f1;
                valueState.update(temp);
                out.collect(temp);
            }
        }
    }
}
