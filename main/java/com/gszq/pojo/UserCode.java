package com.gszq.pojo;

public class UserCode {
    private String stockCcode;
    private String moneyType;
    private String exchangeType;
    private Double marketPrice;
    public String getStockCcode() {
        return stockCcode;
    }
    public void setStockCcode(String stockCcode) {
        this.stockCcode = stockCcode;
    }

    public String getMoneyType() {
        return moneyType;
    }
    public void setMoneyType(String moneyType) {
        this.moneyType = moneyType;
    }


    public String getExchangeType() {
        return exchangeType;
    }
    public void setExchangeType(String exchangeType) {
        this.exchangeType = exchangeType;
    }

    public Double getMarketPrice() {
        return marketPrice;
    }
    public void setMarketPrice(Double marketPrice) {
        this.marketPrice = marketPrice;
    }
}
