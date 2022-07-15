package com.zhouhc.streaming.ch04;

import cn.hutool.core.util.StrUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Java版本的 简单的单词统计
 */
public class WordCount {

    public static final String[] WORDS = new String[]{
            "com.intsmaze.flink.streaming.window.helloword.WordCountTemplate",
            "com.intsmaze.flink.streaming.window.helloword.WordCountTemplate",
            "com.intsmaze.flink.streaming.window.helloword.WordCountTemplate",
            "com.intsmaze.flink.streaming.window.helloword.WordCountTemplate",
    };

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //添加source，也就是数据集，形成流
        DataStreamSource<String> source = environment.fromElements(WORDS);
        //流处理，flatMap就是用于拆分一行的每个单词使用的
        DataStream<Tuple2<String, Integer>> sumStream = source.flatMap(new MyflatMap())
                //按照单词分组，然后 +1
                .keyBy(tuple2 -> tuple2.f0).sum(1);
        //输出流,这里只是简单的输出控制台,每次统计都会刷新控制台，所以它是一个不间断的输出过程
        sumStream.print("word count result");
        //一定要触发执行
        environment.execute("word Count");
    }


    //内部类,不然flink无法识别返回类型
    private static class MyflatMap implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            StrUtil.split(value, '.', -1, true, false).stream().map(String::toLowerCase).map(word -> new Tuple2(word, 1)).forEach(out::collect);
        }
    }
}
