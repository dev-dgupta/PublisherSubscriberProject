package com.ion.java.api.bean;

/**
 * Created by divya.gupta on 01-06-2018.
 */
public class MyPrice {
    // bean to be used to create an MkvSupplyProxy
    private String id;
    private Double ask;
    private Double bid;
    private Double qty;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Double getAsk() {
        return ask;
    }

    public void setAsk(Double ask) {
        this.ask = ask;
    }

    public Double getBid() {
        return bid;
    }

    public void setBid(Double bid) {
        this.bid= bid;
    }

    public Double getQty() {
        return qty;
    }

    public void setQty(Double qty) {
        this.qty = qty;
    }
}