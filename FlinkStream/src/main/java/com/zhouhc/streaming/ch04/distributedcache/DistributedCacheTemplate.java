package com.zhouhc.streaming.ch04.distributedcache;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

/**
 * 分布式缓存, 是的像访问本地一样访问数据
 */
public class DistributedCacheTemplate {
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
        //文件位置，本地文件和服务器文件
        //final String fileName = "file:///C:/Users/f1355/Desktop/test/zhouhc20220613.txt";
        final String fileName = "file:///software/install/flink-1.14.2/zhouhclib/zhouhc20220613.txt";
        //增加分布式文件
        environment.registerCachedFile(fileName, cacheName);
        //添加source，也就是数据集，形成流
        DataStreamSource<String> source = environment.fromElements(WORDS);
        //流处理, flatMap就是用于拆分一行的每个单词使用的, 并且拼接缓存
        DataStream<String> sumStream = source.flatMap(new MyRichFlatMap());
        //输出流,这里只是简单的输出控制台,每次统计都会刷新控制台，所以它是一个不间断的输出过程
        sumStream.print("DistributedCache");
        //一定要触发执行
        environment.execute("DistributedCache");
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
            File cacheFile = getRuntimeContext().getDistributedCache().getFile(cacheName);
            readFile(cacheFile);
        }

        @Override
        public void close() throws Exception {
            System.out.printf("task will be closed!!!!!!!! %n");
        }

        @Override
        public void flatMap(String value, Collector<String> out) throws Exception {
            out.collect(prefix + " " + value);
        }

        /**
         * 读取文件
         *
         * @param file
         */
        private void readFile(File file) {
            try (FileReader fileReader = new FileReader(file);
                 BufferedReader bf = new BufferedReader(fileReader)) {
                String temp;
                while ((temp = bf.readLine()) != null)
                    prefix += temp;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
