package com.zhouhc.streaming.ch06.window.util;


import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

/**
 * 同样简单的时间处理工具类
 */
public class TimeStampUtils {

    public static Timestamp stringToTime(String dateString) throws ParseException {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.CHINA);
        dateFormat.setLenient(false);
        Date timeDate = dateFormat.parse(dateString);
        Timestamp dateTime = new Timestamp(timeDate.getTime());
        return dateTime;
    }

    public static Timestamp toTimestamp(Long time) {
        Timestamp dateTime = new Timestamp(time);
        return dateTime;
    }
}
