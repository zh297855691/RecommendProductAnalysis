package com.cmit.cmhk.entity;

import scala.Int;

import java.io.Serializable;

/**
 * Ӫ��������bean��
 */
public class MarketPlan implements Serializable {

    private static final long serialVersionUID = -6093631689349259219L;
    //Ӫ������ID
    private Integer planID;
    //�ͻ�ID
    private String customerID;
    //Ӫ������״̬
    private String planStatus;
    //��Чʱ��
    private String effectTM;
    //ʧЧʱ��
    private String expireTM;
    //LU��������
    private String planLUDay;

    public String getPlanLUDay() {
        return planLUDay;
    }

    public void setPlanLUDay(String planLUDay) {
        this.planLUDay = planLUDay;
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

    public String getPlanStatus() {
        return planStatus;
    }

    public void setPlanStatus(String planStatus) {
        this.planStatus = planStatus;
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

}
