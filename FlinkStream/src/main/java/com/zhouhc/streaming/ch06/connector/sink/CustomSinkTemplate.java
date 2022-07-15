package com.zhouhc.streaming.ch06.connector.sink;

import cn.hutool.core.io.IoUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.io.FileWriter;

/**
 * 自定义的 sink 操作，其实和source没有什么区别,
 * 唯一区别是每次 都会调用 sink 的 invoke 方法
 * 这里写入的是文件，所以请先确保文件路径存在
 */
public class CustomSinkTemplate {

    private static final String fileName = "F:\\logs\\sink.log";


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<Tuple2<Long, String>> streamSource = env.fromElements(
                new Tuple2<Long, String>(1L, "intsmaze"),
                new Tuple2<Long, String>(2L, "Flink"));

        streamSource.addSink(new CustomSink());
        streamSource.addSink(new CustomRichSink());
        env.execute();
    }


    /**
     * 首先定义一个sink数据源,一般不推荐使用这个，因为每次都需要初始化资源
     */
    private static class CustomSink implements SinkFunction<Tuple2<Long, String>> {
        @Override
        public void invoke(Tuple2<Long, String> value, Context context) throws Exception {
            //SinkFunction.super.invoke(value, context);
            //直接往文件中写数据
            FileWriter fileWriter = new FileWriter(fileName, true);
            fileWriter.write(String.format("CustomSink : %s%n", value));
            fileWriter.flush();
            fileWriter.close();
        }
    }

    /**
     * 推荐使用的sink端，因为只需要初始化一次就可以了
     */
    private static class CustomRichSink extends RichSinkFunction<Tuple2<Long, String>> {
        private FileWriter fileWriter;

        @Override
        public void open(Configuration parameters) throws Exception {
            fileWriter = new FileWriter(fileName, true);
            super.open(parameters);
        }

        @Override
        public void invoke(Tuple2<Long, String> value, Context context) throws Exception {
            //直接往文件中写数据
            fileWriter.write(String.format("CustomRichSink : %s%n", value));
            fileWriter.flush();
        }

        @Override
        public void close() throws Exception {
            if (fileWriter != null)
                fileWriter.close();
            super.close();
        }
    }
}
