package com.zhouhc.streaming.ch06.window.util;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 简单的时间格式化工具
 */
public class TimeUtils {

    public static String getHHmmss(Date date) {
        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss SSS");
        String str = sdf.format(date);
        return "时间:" + str;
    }

    public static String getHHmmss(Long time) {
        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
        String str = sdf.format(new Date(time));
        return "时间:" + str;
    }

    public static String getSs(Date date) {
        SimpleDateFormat sdf = new SimpleDateFormat("ss SSS");
        String str = sdf.format(date);
        return "时间:" + str;
    }

    public static String getSs(Long time) {
        SimpleDateFormat sdf = new SimpleDateFormat("ss SSS");
        String str = sdf.format(new Date(time));
        return str;
    }

}
