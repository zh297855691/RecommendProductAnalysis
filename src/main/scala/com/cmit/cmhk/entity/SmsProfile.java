package com.cmit.cmhk.entity;

public class SmsProfile {

    //营销方案ID
    private Integer planID;
    //客户ID
    private String customerID;
    //用户手机号码
    private String msisdn;
    //国家代码
    private String countryCode;
    //营销短息内容
    private String smsContent;
    //推荐产品ID集合（逗号,分隔）
    private String productIDSet;
    //推荐产品个数
    private Integer productNum;
    //触发标志,1为欢迎短信触发，2为GGSN触发
    private String flag;

    public String getSendLanguage() {
        return sendLanguage;
    }

    public void setSendLanguage(String sendLanguage) {
        this.sendLanguage = sendLanguage;
    }

    //短信内容语言
    private String sendLanguage;



    public String getProductIDSet() {
        return productIDSet;
    }

    public void setProductIDSet(String productIDSet) {
        this.productIDSet = productIDSet;
    }

    public Integer getProductNum() {
        return productNum;
    }

    public void setProductNum(Integer productNum) {
        this.productNum = productNum;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    public Integer getPlanID() {
        return planID;
    }

    public void setPlanID(Integer planID) {
        this.planID = planID;
    }

    public String getCustomerID() {
        return customerID;
    }

    public void setCustomerID(String customerID) {
        this.customerID = customerID;
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

    public String getSmsContent() {
        return smsContent;
    }

    public void setSmsContent(String smsContent) {
        this.smsContent = smsContent;
    }
}
