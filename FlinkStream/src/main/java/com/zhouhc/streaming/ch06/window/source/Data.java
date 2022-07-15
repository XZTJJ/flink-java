package com.zhouhc.streaming.ch06.window.source;

import com.zhouhc.streaming.ch06.window.time.bean.EventBean;

import java.util.Date;

//默认的数据源
public class Data {
    public static Date date = new Date();

    public static final EventBean[] BEANS = new EventBean[]{
            new EventBean("0.money", date.getTime()),
            new EventBean("1.money", date.getTime() + 10000),
            new EventBean("2.money", date.getTime() + 20000),
            new EventBean("3-nosleep", date.getTime() + 30000),
            new EventBean("4.late", date.getTime() + 20000),
            new EventBean("5.money", date.getTime() + 40000),
            new EventBean("6.money", date.getTime() + 50000),
            new EventBean("7-nosleep", date.getTime() + 60000),
            new EventBean("8-nosleep-late", date.getTime() + 50000),
            new EventBean("9.late", date.getTime() + 50000),
            new EventBean("10.money", date.getTime() + 70000),
            new EventBean("11.money", date.getTime() + 80000),
            new EventBean("12-nosleep", date.getTime() + 90000),
            new EventBean("13-late-abandon", date.getTime() + 50000),
            new EventBean("14.money", date.getTime() + 100000),
            new EventBean("15.money", date.getTime() + 110000),
    };
}
