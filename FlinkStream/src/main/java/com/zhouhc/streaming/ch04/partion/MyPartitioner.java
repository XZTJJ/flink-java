package com.zhouhc.streaming.ch04.partion;

import cn.hutool.core.util.StrUtil;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * 自定义分区
 */
public class MyPartitioner implements Partitioner<Tuple3<String, Integer, String>> {
    /**
     * numPartitions 下一个操作符的并行度的大小，并行度区间从 0 开始的哈
     */
    @Override
    public int partition(Tuple3<String, Integer, String> key, int numPartitions) {
        if (StrUtil.startWith(key.f0, "185"))
            return 0;
        else if (StrUtil.startWith(key.f0, "155"))
            return 1;
        else
            return 2;
    }
}
