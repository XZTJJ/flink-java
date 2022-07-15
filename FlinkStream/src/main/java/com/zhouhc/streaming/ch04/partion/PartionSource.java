package com.zhouhc.streaming.ch04.partion;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * 自定义数据源
 */
public class PartionSource extends RichSourceFunction<Tuple3<String, Integer, String>> {
    @Override
    public void run(SourceContext<Tuple3<String, Integer, String>> ctx) throws Exception {
        List<Tuple3<String, Integer, String>> tuple3s = new ArrayList<Tuple3<String, Integer, String>>();
        tuple3s.add(new Tuple3<String, Integer, String>("185XXX", 899, "2018"));
        tuple3s.add(new Tuple3<String, Integer, String>("155XXX", 1111, "2019"));
        tuple3s.add(new Tuple3<String, Integer, String>("155XXX", 1199, "2019"));
        tuple3s.add(new Tuple3<String, Integer, String>("185XXX", 1899, "2017"));
        tuple3s.add(new Tuple3<String, Integer, String>("138XXX", 19, "2019"));
        tuple3s.add(new Tuple3<String, Integer, String>("138XXX", 399, "2020"));
        //开始遍历循环
        for (int i = 0; i < tuple3s.size(); i++)
            ctx.collect(tuple3s.get(i));
        String subTaskName = getRuntimeContext().getTaskNameWithSubtasks();
        System.out.printf("source所遇子任务名称 : %s %n", subTaskName);
    }

    @Override
    public void cancel() {

    }
}
