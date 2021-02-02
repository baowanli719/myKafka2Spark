package com.gszq.pojo;

public class StockTodayIncome {
    private String tradeValue;
    private String lastDayAsset;
    private String todayIncome;
    private String realAsset;
    private String stockCode;

    public String getStockCode() {
        return stockCode;
    }
    public void setStockCode(String stockCode) {
        this.stockCode = stockCode;
    }
    public String getTradeValue() {
        return tradeValue;
    }
    public void setTradeValue(String tradeValue) {
        this.tradeValue = tradeValue;
    }
    public String getLastDayAsset() {
        return lastDayAsset;
    }
    public void setLastDayAsset(String lastDayAsset) {
        this.lastDayAsset = lastDayAsset;
    }

    public String getTotalAsset() {
        return realAsset;
    }
    public void setRealAsset(String realAsset) {
        this.realAsset = realAsset;
    }

    public String getTodayIncome() {
        return todayIncome;
    }
    public void setTodayIncome(String todayIncome) {
        this.todayIncome = todayIncome;
    }

    public StockTodayIncome() {
        super();
        // TODO Auto-generated constructor stub
    }
}
