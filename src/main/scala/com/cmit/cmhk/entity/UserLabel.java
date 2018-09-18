package com.cmit.cmhk.entity;

import java.io.Serializable;

public class UserLabel implements Serializable {

    private static final long serialVersionUID = 5683200548715649267L;
    private Integer labelKey;
    //用户标签ID
    private String labelID;
    //客户ID
    private String customerID;
    //生效时间
    private String effectTM;
    //失效时间
    private String expireTM;
    //标签条件组合
    private String conditionResult;

    public void setLabelID(String labelID) {
        this.labelID = labelID;
    }

    public Integer getLabelKey() {
        return labelKey;
    }

    public void setLabelKey(Integer labelKey) {
        this.labelKey = labelKey;
    }

    public String getLabelID() {
        return labelID;
    }

    public String getCustomerID() {
        return customerID;
    }

    public void setCustomerID(String customerID) {
        this.customerID = customerID;
    }

    public String getEffectTM() {
        return effectTM;
    }

    public void setEffectTM(String effectTM) {
        this.effectTM = effectTM;
    }

    public String getExpireTM() {
        return expireTM;
    }

    public void setExpireTM(String expireTM) {
        this.expireTM = expireTM;
    }

    public String getConditionResult() {
        return conditionResult;
    }

    public void setConditionResult(String conditionResult) {
        this.conditionResult = conditionResult;
    }

    @Override
    public String toString() {
        return "UserLabel{" +
                "labelKey=" + labelKey +
                ", labelID='" + labelID + '\'' +
                ", customerID='" + customerID + '\'' +
                ", effectTM='" + effectTM + '\'' +
                ", expireTM='" + expireTM + '\'' +
                ", conditionResult='" + conditionResult + '\'' +
                '}';
    }
}
