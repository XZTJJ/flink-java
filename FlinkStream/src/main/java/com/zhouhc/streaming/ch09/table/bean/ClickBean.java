package com.zhouhc.streaming.ch09.table.bean;

import com.zhouhc.streaming.ch06.window.util.TimeStampUtils;

import java.sql.Timestamp;
import java.text.ParseException;

public class ClickBean {

    public String user;

    public Timestamp time;

    public String url;

    public int id;

    public ClickBean() {
    }

    public ClickBean(int id, String user, String url, String time) throws ParseException {
        this.user = user;
        this.url = url;
        this.time = TimeStampUtils.stringToTime(time);
        this.id = id;
    }


    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Timestamp getTime() {
        return time;
    }

    public void setTime(Timestamp time) {
        this.time = time;
    }


    @Override
    public String toString() {
        return "ClickBean{" +
                "user='" + user + '\'' +
                ", time=" + time +
                ", url='" + url + '\'' +
                '}';
    }
}
