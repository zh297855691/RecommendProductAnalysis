package com.cmit.cmhk.entity;

public class UserRealTimeLabel {

    //�ֻ�����
    private String msisdn = "";
    //IMSI��
    private String imsi = "";
    //���Ҵ���
    private String countryCode = "";
    //��ӭ����ʱ��
    private String luTime = "";
    //����Դ
    private String source = "";
    //ʵʱ������ƷID
    private String orderProductID = "";
    //�Ƿ�Ϊ�������û�
    private String isWhiteList = "1";
    //�󸶷�����
    private String isPayList = "";
    //�·Ѽƻ�
    private String monthlyFee = "";

    public String getMsisdn() {
        return msisdn;
    }

    public void setMsisdn(String msisdn) {
        this.msisdn = msisdn;
    }

    public String getImsi() {
        return imsi;
    }

    public void setImsi(String imsi) {
        this.imsi = imsi;
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

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getOrderProductID() {
        return orderProductID;
    }

    public void setOrderProductID(String orderProductID) {
        this.orderProductID = orderProductID;
    }

    public String getIsWhiteList() {
        return isWhiteList;
    }

    public void setIsWhiteList(String isWhiteList) {
        this.isWhiteList = isWhiteList;
    }

    public String getIsPayList() {
        return isPayList;
    }

    public void setIsPayList(String isPayList) {
        this.isPayList = isPayList;
    }

    public String getMonthlyFee() {
        return monthlyFee;
    }

    public void setMonthlyFee(String monthlyFee) {
        this.monthlyFee = monthlyFee;
    }

    @Override
    public String toString() {
        return "UserRealTimeLabel{" +
                "msisdn='" + msisdn + '\'' +
                ", imsi='" + imsi + '\'' +
                ", countryCode='" + countryCode + '\'' +
                ", luTime='" + luTime + '\'' +
                ", source='" + source + '\'' +
                ", orderProductID='" + orderProductID + '\'' +
                ", isWhiteList='" + isWhiteList + '\'' +
                ", isPayList='" + isPayList + '\'' +
                ", monthlyFee='" + monthlyFee + '\'' +
                '}';
    }
}
