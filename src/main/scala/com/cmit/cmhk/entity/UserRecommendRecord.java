package com.cmit.cmhk.entity;

public class UserRecommendRecord {

    //营销方案ID
    private String planID = "";
    //手机号码
    private String msisdn = "";
    //出访地国家代码
    private String countryCode = "";
    //产品ID
    private String luTime = "";
    //优先级
    private String pri = "";
    //是否推送
    private String isPush = "0";
    //是否激活
    private String isAvtivity = "";
    //产品ID
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
