package com.zhouhc.streaming.ch06.window.source;

import com.zhouhc.streaming.ch06.window.util.TimeUtils;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.SourceFunction;


/**
 * 一个window窗口数据源
 */
public class SourceForWindow implements SourceFunction<Tuple3<String, Integer, String>> {
    public static final String[] WORDS = new String[]{
            "intsmaze", "intsmaze", "intsmaze", "intsmaze", "intsmaze", "java", "flink", "flink", "flink", "intsmaze", "intsmaze", "hadoop", "hadoop", "spark"
    };

    private static final long serialVersionUID = 1L;
    private volatile boolean isRunning = true;
    private long sleepTime;
    private Boolean stopSession;

    public SourceForWindow(long sleepTime, Boolean stopSession) {
        this.sleepTime = sleepTime;
        this.stopSession = stopSession;
    }

    @Override
    public void run(SourceContext<Tuple3<String, Integer, String>> ctx) throws Exception {
        int count = 0;
        while (isRunning) {
            String word = WORDS[count % WORDS.length];
            String time = TimeUtils.getHHmmss(System.currentTimeMillis());
            Tuple3<String, Integer, String> elem = new Tuple3<String, Integer, String>(word, count, time);
            System.out.printf("send data : %s %n", elem);
            ctx.collect(elem);
            if (stopSession && count % WORDS.length == 0 && count != 0)
                Thread.sleep(10000);
            else
                Thread.sleep(sleepTime);
            count++;
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
