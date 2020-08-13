package com.sunweiye.flink.java;

public class WCBean {
    private String name;
    private int count;

    @Override
    public String toString() {
        return "WCBean{" +
                "name='" + name + '\'' +
                ", count=" + count +
                '}';
    }

    public String getName() {
        return name;
    }

    public int getCount() {
        return count;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public WCBean(String name, int count) {
        this.name = name;
        this.count = count;
    }
    public WCBean() {
    }
}
