package com.cmit.cmhk.entity;

import java.io.Serializable;
import java.util.ArrayList;

public class ProductInfo implements Serializable {

    private static final long serialVersionUID = 5088089940164074817L;
    /**
     * 序列号，主键
     */
    private Integer productKey;
    //营销产品ID
    private String productID;
    //客户ID
    private String customerID;
    //营销产品中文名(繁体)
    private String productName;
    //营销产品英文名
    private String productNameEN;
    //产品类型：1为语音包，2位流量包
    private String productType;
    //产品价格
    private String productPrice;
    //产品包天数
    private String productDays;
    //办理短信编码
    private String productSMSCD;
    //办理短码
    private String productUSSDCD;
    //生效时间
    private String effectTM;
    //失效时间
    private String expireTM;
    //产品覆盖国家代码
    private ArrayList countryCodes;

    public Integer getProductKey() {
        return productKey;
    }

    public void setProductKey(Integer productKey) {
        this.productKey = productKey;
    }

    public String getProductID() {
        return productID;
    }

    public void setProductID(String productID) {
        this.productID = productID;
    }

    public String getCustomerID() {
        return customerID;
    }

    public void setCustomerID(String customerID) {
        this.customerID = customerID;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public String getProductNameEN() {
        return productNameEN;
    }

    public void setProductNameEN(String productNameEN) {
        this.productNameEN = productNameEN;
    }

    public String getProductType() {
        return productType;
    }

    public void setProductType(String productType) {
        this.productType = productType;
    }

    public String getProductPrice() {
        return productPrice;
    }

    public void setProductPrice(String productPrice) {
        this.productPrice = productPrice;
    }

    public String getProductDays() {
        return productDays;
    }

    public void setProductDays(String productDays) {
        this.productDays = productDays;
    }

    public String getProductSMSCD() {
        return productSMSCD;
    }

    public void setProductSMSCD(String productSMSCD) {
        this.productSMSCD = productSMSCD;
    }

    public String getProductUSSDCD() {
        return productUSSDCD;
    }

    public void setProductUSSDCD(String productUSSDCD) {
        this.productUSSDCD = productUSSDCD;
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

    public ArrayList getCountryCodes() {
        return countryCodes;
    }

    public void setCountryCodes(ArrayList countryCodes) {
        this.countryCodes = countryCodes;
    }


}
