package com.zhouhc.streaming.ch09.table.bean;

import com.zhouhc.streaming.ch06.window.util.TimeStampUtils;

import java.sql.Timestamp;
import java.text.ParseException;

/**
 *
 */
public class RateBean {

    public String currency;

    public Timestamp time;

    public int rate;

    public int id;

    public RateBean() {
    }

    public RateBean(int id, String currency, int rate, String time) throws ParseException {
        this.currency = currency;
        this.rate = rate;
        this.time = TimeStampUtils.stringToTime(time);
        this.id = id;
    }

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public Timestamp getTime() {
        return time;
    }

    public void setTime(Timestamp time) {
        this.time = time;
    }

    public int getRate() {
        return rate;
    }

    public void setRate(int rate) {
        this.rate = rate;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }
}
