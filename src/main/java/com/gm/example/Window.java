package com.gm.example;

public class Window {

    private String attrField;

    private long maxOutOfOrderness;

    public Window(String attrField, long maxOutOfOrderness) {
        this.attrField = attrField;
        this.maxOutOfOrderness = maxOutOfOrderness;
    }

    public String getAttrField() {
        return attrField;
    }

    public void setAttrField(String attrField) {
        this.attrField = attrField;
    }

    public long getMaxOutOfOrderness() {
        return maxOutOfOrderness;
    }

    public void setMaxOutOfOrderness(long maxOutOfOrderness) {
        this.maxOutOfOrderness = maxOutOfOrderness;
    }
}
