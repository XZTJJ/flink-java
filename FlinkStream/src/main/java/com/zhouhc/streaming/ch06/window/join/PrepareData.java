package com.zhouhc.streaming.ch06.window.join;

import com.zhouhc.streaming.ch06.window.util.TimeStampUtils;

import java.sql.Timestamp;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

/**
 * 数据源
 */
public class PrepareData {


    public static List<ClickBean> getClicksData() throws ParseException {
        List<ClickBean> clickList = new ArrayList();
        clickList.add(new ClickBean(1, "张三", "./intsmaze", "2019-07-28 12:00:00"));
        clickList.add(new ClickBean(2, "李四", "./flink", "2019-07-28 12:05:05"));
        clickList.add(new ClickBean(3, "张三", "./stream", "2019-07-28 12:45:08"));
        clickList.add(new ClickBean(4, "李四", "./intsmaze", "2019-07-28 13:01:00"));
        clickList.add(new ClickBean(5, "王五", "./flink", "2019-07-28 13:04:00"));
        return clickList;
    }


    public static List<Trade> getTradeData() throws ParseException {
        List<Trade> tradeList = new ArrayList();
        tradeList.add(new Trade("张三", 38, "安卓手机", "2019-07-28 12:20:00"));
        tradeList.add(new Trade("王五", 45, "苹果手机", "2019-07-28 12:30:00"));
        tradeList.add(new Trade("张三", 18, "台式机", "2019-07-28 13:20:00"));
        tradeList.add(new Trade("王五", 23, "笔记本", "2019-07-28 13:58:00"));
        return tradeList;
    }


    //浏览数据
    public static class ClickBean {

        public String user;

        public Timestamp visitTime;

        public String url;

        public int id;

        public ClickBean() {
        }

        public ClickBean(int id, String user, String url, String visitTime) throws ParseException {
            this.user = user;
            this.url = url;
            this.visitTime = TimeStampUtils.stringToTime(visitTime);
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

        public Timestamp getVisitTime() {
            return visitTime;
        }

        public void setVisitTime(Timestamp visitTime) {
            this.visitTime = visitTime;
        }

        @Override
        public String toString() {
            return "ClickBean{" +
                    "user='" + user + '\'' +
                    ", visitTime=" + visitTime +
                    ", url='" + url + '\'' +
                    '}';
        }
    }

    //订单类数据
    public static class Trade {

        private String name;

        private long amount;

        private String client;

        private Timestamp tradeTime;

        public Trade() {
        }

        public Trade(String name, long amount, String client, String tradeTime) throws ParseException {
            this.name = name;
            this.amount = amount;
            this.client = client;
            this.tradeTime = TimeStampUtils.stringToTime(tradeTime);
        }

        public Timestamp getTradeTime() {
            return tradeTime;
        }

        public void setTradeTime(Timestamp tradeTime) {
            this.tradeTime = tradeTime;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public long getAmount() {
            return amount;
        }

        public void setAmount(long amount) {
            this.amount = amount;
        }

        public String getClient() {
            return client;
        }

        public void setClient(String client) {
            this.client = client;
        }

        @Override
        public String toString() {
            return "Trade{" +
                    "name='" + name + '\'' +
                    ", amount=" + amount +
                    ", client='" + client + '\'' +
                    ", tradeTime=" + tradeTime +
                    '}';
        }
    }
}
