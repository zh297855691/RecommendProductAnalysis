package com.cmit.cmhk.hbase;

public class OrderingInfo {

    private String productID;
    private String userID;
    private String product_ACTIVATION_DATE; //��Ʒ������Ч�ڿ�ʼ����
    private String product_DEACTIVATION_DATE; //��Ʒ������Ч�ڽ�ֹ����
    private String activationDate; //��Ʒ���ʼ����
    private String deactivationDate; //��Ʒ����ʧЧ��ֹ����
    private String flag; //�û������Ĳ�Ʒ�Ƿ��Ѿ�����,���Ѽ���Ϊ100

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    public String getProductID() {
        return productID;
    }

    public String getUserID() {
        return userID;
    }

    public String getProduct_ACTIVATION_DATE() {
        return product_ACTIVATION_DATE;
    }

    public String getProduct_DEACTIVATION_DATE() {
        return product_DEACTIVATION_DATE;
    }

    public void setProductID(String productID) {
        this.productID = productID;
    }

    public void setUserID(String userID) {
        this.userID = userID;
    }

    public void setProduct_ACTIVATION_DATE(String product_ACTIVATION_DATE) {
        this.product_ACTIVATION_DATE = product_ACTIVATION_DATE;
    }

    public void setProduct_DEACTIVATION_DATE(String product_DEACTIVATION_DATE) {
        this.product_DEACTIVATION_DATE = product_DEACTIVATION_DATE;
    }

    public void setActivationDate(String activationDate) {
        this.activationDate = activationDate;
    }

    public void setDeactivationDate(String deactivationDate) {
        this.deactivationDate = deactivationDate;
    }

    public String getActivationDate() {
        return activationDate;
    }

    public String getDeactivationDate() {
        return deactivationDate;
    }

}