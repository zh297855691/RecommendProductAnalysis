package com.cmit.cmhk.entity;

public class UserRecommendRecord {

    //Ӫ������ID
    private String planID = "";
    //�ֻ�����
    private String msisdn = "";
    //���õع��Ҵ���
    private String countryCode = "";
    //��ƷID
    private String luTime = "";
    //���ȼ�
    private String pri = "";
    //�Ƿ�����
    private String isPush = "0";
    //�Ƿ񼤻�
    private String isAvtivity = "";
    //��ƷID
    private String productID = "";

    public String getProductID() {
        return productID;
    }

    public void setProductID(String productID) {
        this.productID = productID;
    }

    public String getPlanID() {
        return planID;
    }

    public void setPlanID(String planID) {
        this.planID = planID;
    }

    public String getMsisdn() {
        return msisdn;
    }

    public void setMsisdn(String msisdn) {
        this.msisdn = msisdn;
    }

    public String getCountryCode() {
        return countryCode;
    }

    public void setCountryCode(String countryCode) {
        this.countryCode = countryCode;
    }

    public String getLuTime() {
        return luTime;
    }

    public void setLuTime(String luTime) {
        this.luTime = luTime;
    }

    public String getPri() {
        return pri;
    }

    public void setPri(String pri) {
        this.pri = pri;
    }

    public String getIsPush() {
        return isPush;
    }

    public void setIsPush(String isPush) {
        this.isPush = isPush;
    }

    public String getIsAvtivity() {
        return isAvtivity;
    }

    public void setIsAvtivity(String isAvtivity) {
        this.isAvtivity = isAvtivity;
    }
}
