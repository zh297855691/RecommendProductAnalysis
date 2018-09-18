package com.cmit.cmhk.hbase;

public class OrderingInfo {

    private String productID;
    private String userID;
    private String product_ACTIVATION_DATE; //产品订购有效期开始日期
    private String product_DEACTIVATION_DATE; //产品订购有效期截止日期
    private String activationDate; //产品激活开始日期
    private String deactivationDate; //产品激活失效截止日期
    private String flag; //用户订购的产品是否已经激活,若已激活为100

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