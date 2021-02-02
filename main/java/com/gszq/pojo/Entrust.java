package com.gszq.pojo;

import java.math.BigDecimal;

//定义一个entrust对象
public class Entrust {
    private String rowKey;
    private String timeload;
    private String init_date;
    private String rowTime;
    private BigDecimal rowValue;


    //get方法：从实体类对象中获取属性值
    public String getRowKey() {
        return rowKey;
    }
    //set方法：为实体类对象的属性赋值
    public void setRowkey(String rowKey) {
        this.rowKey=rowKey;
    }

    public String getTimeload() {
        return timeload;
    }
    public void setTimeload(String timeload) {
        this.timeload=timeload;
    }

    public String getInit_date() {
        return init_date;
    }
    public void setInit_date(String init_date) {
        this.init_date=init_date;
    }

    public String getRowTime() {
        return rowTime;
    }
    public void setRowTime(String rowTime) {
        this.rowTime=rowTime;
    }

    public BigDecimal getRowValue() {
        return rowValue;
    }
    public void setRowValue(BigDecimal rowValue) {
        this.rowValue=rowValue;
    }

    public Entrust() {

    }
}