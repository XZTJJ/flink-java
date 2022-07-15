package com.zhouhc.streaming.ch04.param;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 *
 */
public class ExceutionConfigTemplate {
    //字段
    private static final String[] WORDS = new String[]{
            "com.intsmaze.flink.streaming.window.helloword.WordCountTemplate1",
            "com.intsmaze.flink.streaming.window.helloword.WordCountTemplate2",
            "com.intsmaze.flink.streaming.window.helloword.WordCountTemplate3",
            "com.intsmaze.flink.streaming.window.helloword.WordCountTemplate4",
    };

    private static final String cacheName = "localFileCache";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(3);
        //从传入的参数读取,参数一般为 --prefix prefix格式，类似linux的参数，通过key获得对应的参数。
        Configuration configuration = ParameterTool.fromArgs(args).getConfiguration();
        configuration.setString("second","second");
        //文件位置，配置文件
        ExecutionConfig config = environment.getConfig();
        config.setGlobalJobParameters(configuration);
        //添加source，也就是数据集，形成流
        DataStreamSource<String> source = environment.fromElements(WORDS);
        //流处理, flatMap就是用于拆分一行的每个单词使用的, 并且拼接缓存
        DataStream<String> sumStream = source.flatMap(new MyRichFlatMap());
        //输出流,这里只是简单的输出控制台,每次统计都会刷新控制台，所以它是一个不间断的输出过程
        sumStream.print("ExceutionConfig");
        //一定要触发执行
        environment.execute("ExceutionConfig");
    }

    /**
     * 内部处理类
     */
    private static class MyRichFlatMap extends RichFlatMapFunction<String, String> {
        private String prefix = "";

        //读取缓存
        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.printf("task will be start!!!!!!!! %n");
            Configuration configuration = (Configuration)getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
            prefix += configuration.getString(ConfigOptions.key("prefix").stringType().defaultValue("aaa")) + " ";
            prefix += configuration.getString(ConfigOptions.key("second").stringType().defaultValue("s"));
        }

        @Override
        public void close() throws Exception {
            System.out.printf("task will be closed!!!!!!!! %n");
        }

        @Override
        public void flatMap(String value, Collector<String> out) throws Exception {
            out.collect(prefix + " " + value);
        }
        
    }
}
