package com.zhouhc.streaming.ch06.sideoutput;

import cn.hutool.core.util.StrUtil;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.junit.Test;

/**
 * 侧边输出功能，也是推荐使用的功能
 */
public class SideOutputTemplate {

    public static final String[] DATA = new String[]{
            "In addition to the main stream that results from DataStream operations",
            "When using side outputs",
            "We recommend you",
            "you first need to define an OutputTag that will be used to identify a side output stream"
    };

    /**
     * 测试复制流的功能
     *
     * @throws Exception
     */
    @Test
    public void testCopyStream() throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> sourceDataStream = environment.fromElements(DATA);
        //小于20的流元素
        DataStream<String> rejectedStream = sourceDataStream.filter(value -> value.length() < 20 ? true : false);
        rejectedStream.print("testCopyStream rejectedStream");
        //大于20的流元素
        DataStream<Tuple2<String, Integer>> resultStream = sourceDataStream.filter(value -> value.length() >= 20 ? true : false)
                .map(value -> new Tuple2<String, Integer>(value, StrUtil.split(value, ' ').size()),
                        TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
                        }));
        //结算
        resultStream.print("count");
        environment.execute("testCopyStream");
    }


    /**
     * 使用侧标输出功能
     */
    @Test
    public void testSideOutput() throws Exception {
        //测试数据
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> sourceDataStream = environment.fromElements(DATA);
        //进行侧边输出
        SingleOutputStreamOperator<String> tempStream = sourceDataStream.process(new ProcessFunction<String, String>() {
            //进行侧边输出或者使用直接输出到主数据流中
            @Override
            public void processElement(String value, ProcessFunction<String, String>.Context ctx, Collector<String> out) throws Exception {
                if (value.length() < 20)
                    ctx.output(new OutputTag<String>("rejectedStream") {
                    }, value);
                else
                    out.collect(value);
            }
        });

        //两个获取数据流
        DataStream<String> rejectedStream = tempStream.getSideOutput(new OutputTag<String>("rejectedStream") {
        });
        rejectedStream.print("rejectedStream");
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultStream = tempStream.map(value -> new Tuple2<String, Integer>(value, StrUtil.split(value, ' ').size()),
                TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
                }));
        resultStream.print("resultStream");
        environment.execute("testSideOutput");
    }
}
