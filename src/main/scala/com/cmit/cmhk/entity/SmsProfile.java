package com.cmit.cmhk.entity;

public class SmsProfile {

    //Ӫ������ID
    private Integer planID;
    //�ͻ�ID
    private String customerID;
    //�û��ֻ�����
    private String msisdn;
    //���Ҵ���
    private String countryCode;
    //Ӫ����Ϣ����
    private String smsContent;
    //�Ƽ���ƷID���ϣ�����,�ָ���
    private String productIDSet;
    //�Ƽ���Ʒ����
    private Integer productNum;
    //������־,1Ϊ��ӭ���Ŵ�����2ΪGGSN����
    private String flag;

    public String getSendLanguage() {
        return sendLanguage;
    }

    public void setSendLanguage(String sendLanguage) {
        this.sendLanguage = sendLanguage;
    }

    //������������
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
