package com.cmit.cmhk.entity;

import java.io.Serializable;
import java.util.ArrayList;

public class ProductInfo implements Serializable {

    private static final long serialVersionUID = 5088089940164074817L;
    /**
     * ���кţ�����
     */
    private Integer productKey;
    //Ӫ����ƷID
    private String productID;
    //�ͻ�ID
    private String customerID;
    //Ӫ����Ʒ������(����)
    private String productName;
    //Ӫ����ƷӢ����
    private String productNameEN;
    //��Ʒ���ͣ�1Ϊ��������2λ������
    private String productType;
    //��Ʒ�۸�
    private String productPrice;
    //��Ʒ������
    private String productDays;
    //������ű���
    private String productSMSCD;
    //�������
    private String productUSSDCD;
    //��Чʱ��
    private String effectTM;
    //ʧЧʱ��
    private String expireTM;
    //��Ʒ���ǹ��Ҵ���
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
