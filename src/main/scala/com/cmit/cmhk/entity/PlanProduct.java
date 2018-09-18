package com.cmit.cmhk.entity;

import java.io.Serializable;

public class PlanProduct implements Serializable {

    private static final long serialVersionUID = -6079520158883963110L;
    //Ӫ������ID
    private Integer planID;
    //�Ƽ�Ӫ����ƷID
    private String productID;
    //�Ƽ�Ӫ����Ʒ���ȼ���1����8����Ĭ��Ϊ1
    private String productPriority;
    //�û���ǩID
    private String labelID;
    //�����ϵ
    private String instructions;
    //ÿ���Ʒ����������Ĭ��1000
    private String sendLimit;

    public String getSendLimit() {
        return sendLimit;
    }

    public void setSendLimit(String sendLimit) {
        this.sendLimit = sendLimit;
    }

    public String getInstructions() {
        return instructions;
    }

    public void setInstructions(String instructions) {
        this.instructions = instructions;
    }

    public String getProductID() {
        return productID;
    }

    public void setProductID(String productID) {
        this.productID = productID;
    }

    public Integer getPlanID() {
        return planID;
    }

    public String getProductPriority() {
        return productPriority;
    }

    public void setProductPriority(String productPriority) {
        this.productPriority = productPriority;
    }

    public void setPlanID(Integer planID) {
        this.planID = planID;
    }

    public String getLabelID() {
        return labelID;
    }

    public void setLabelID(String labelID) {
        this.labelID = labelID;
    }
}
