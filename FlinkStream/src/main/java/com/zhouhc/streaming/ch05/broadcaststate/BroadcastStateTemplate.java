package com.zhouhc.streaming.ch05.broadcaststate;

import cn.hutool.core.util.StrUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 广播状态的数据流处理
 */
public class BroadcastStateTemplate {

    //获取相关执行环境
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(2);
        //设置主流数据源
        DataStream<Date> mainSource = environment.addSource(new CustomMainSource(1000));
        DataStream<Tuple2<Integer, String>> broadcastSource = environment.addSource(new BroadCastSource(5000));
        //创建广播保存信息
        final MapStateDescriptor<Integer, String> mapStateDescriptor = new MapStateDescriptor<Integer, String>("braodcast",
                TypeInformation.of(new TypeHint<Integer>() {
                }), TypeInformation.of(new TypeHint<String>() {
        }));
        //broadcastSource变成广播流
        BroadcastStream<Tuple2<Integer, String>> broadcastStream = broadcastSource.broadcast(mapStateDescriptor);
        //和主流进行连接
        DataStream<Tuple2<String, String>> processStream = mainSource.connect(broadcastStream).process(new MyBroadcastProcessFunction());
        processStream.print("BroadcastStateTemplate:");
        environment.execute("BroadcastStateTemplate");
    }


    /**
     * 主数据流对应的广播处理函数,需要处理广播流元素和非广播流元素
     */
    private static class MyBroadcastProcessFunction extends BroadcastProcessFunction<Date, Tuple2<Integer, String>, Tuple2<String, String>> {
        private transient MapStateDescriptor<Integer, String> mapStateDescriptor;

        @Override
        public void open(Configuration parameters) throws Exception {
            mapStateDescriptor = new MapStateDescriptor<Integer, String>("braodcast",
                    TypeInformation.of(new TypeHint<Integer>() {
                    }), TypeInformation.of(new TypeHint<String>() {
            }));
        }

        //对每个主流元素进行处理
        @Override
        public void processElement(Date value, BroadcastProcessFunction<Date, Tuple2<Integer, String>, Tuple2<String, String>>.ReadOnlyContext ctx,
                                   Collector<Tuple2<String, String>> out) throws Exception {
            ReadOnlyBroadcastState<Integer, String> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
            StringBuffer sb = new StringBuffer();
            String currentFormat = null;
            //遍历规则
            for (Map.Entry<Integer, String> entry : broadcastState.immutableEntries()) {
                currentFormat = entry.getValue();
                sb.append(" ").append(entry.getKey()).append(" : ").append(currentFormat).append(" , ");
            }
            //进行广播流中的规则处理主流中的数据
            if (StrUtil.isNotBlank(currentFormat)) {
                String oriData = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(value);
                String broadData = new SimpleDateFormat(currentFormat).format(value);
                System.out.printf("广播流中总共有的元素有: %s %n", sb.toString());
                //往下游发送数据
                out.collect(new Tuple2<String, String>(oriData, broadData));
            }
        }

        //处理广播流里面的元素,并且需要将广播元素应用到主数据流中
        @Override
        public void processBroadcastElement(Tuple2<Integer, String> value, BroadcastProcessFunction<Date, Tuple2<Integer, String>, Tuple2<String, String>>.Context ctx,
                                            Collector<Tuple2<String, String>> out) throws Exception {
            //获取广播元素状态,并且添加广播流
            BroadcastState<Integer, String> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
            broadcastState.put(value.f0, value.f1);
            //下游元素的输出，可做可不做的
            out.collect(new Tuple2<String, String>("新增广播元素为: ", value.toString()));
        }
    }

    /**
     * 主数据源
     */
    private static class CustomMainSource implements SourceFunction<Date> {
        //每个元素间隔时间
        private final int sleep;

        public CustomMainSource(int sleep) {
            this.sleep = sleep;
        }

        //需要一直不断的发
        @Override
        public void run(SourceContext<Date> ctx) throws Exception {
            while (true) {
                ctx.collect(new Date());
                TimeUnit.MILLISECONDS.sleep(sleep);
            }
        }

        @Override
        public void cancel() {

        }
    }

    /**
     * 广播数据源
     */
    private static class BroadCastSource implements SourceFunction<Tuple2<Integer, String>> {
        //定义时间格式
        private final String[] formats = new String[]{
                "yyyy-MM-dd HH:mm", "yyyy-MM-dd HH", "yyyy-MM-dd", "yyyy-MM", "yyyy"
        };
        //每个元素间隔时间
        private final int sleep;

        public BroadCastSource(int sleep) {
            this.sleep = sleep;
        }

        @Override
        public void run(SourceContext<Tuple2<Integer, String>> ctx) throws Exception {
            int count = 0;
            while (true) {
                for (String format : formats) {
                    ctx.collect(new Tuple2<Integer, String>(++count, format));
                    TimeUnit.MILLISECONDS.sleep(sleep);
                }
            }
        }

        @Override
        public void cancel() {

        }
    }
}
