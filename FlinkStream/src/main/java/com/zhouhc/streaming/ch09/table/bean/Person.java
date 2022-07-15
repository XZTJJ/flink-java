package com.zhouhc.streaming.ch09.table.bean;

public class Person {

    private String name;
    private long age;
    private String city;

    public Person() {
    }

    public Person(String name, long age, String city) {
        this.name = name;
        this.age = age;
        this.city = city;
    }


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getAge() {
        return age;
    }

    public void setAge(long age) {
        this.age = age;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    @Override
    public String toString() {
        return "Person{" +
                "name='" + name + '\'' +
                ", age=" + age +
                ", city='" + city + '\'' +
                '}';
    }
}
