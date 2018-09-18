package com.cmit.cmhk.entity;

import java.io.Serializable;

public class PlanProduct implements Serializable {

    private static final long serialVersionUID = -6079520158883963110L;
    //营销方案ID
    private Integer planID;
    //推荐营销产品ID
    private String productID;
    //推荐营销产品优先级，1级到8级，默认为1
    private String productPriority;
    //用户标签ID
    private String labelID;
    //互斥关系
    private String instructions;
    //每天产品推送限量，默认1000
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
