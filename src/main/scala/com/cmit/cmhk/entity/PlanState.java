package com.cmit.cmhk.entity;

public class PlanState {
    //营销方案ID+修改内容
    private String flagID;
    //客户ID
//    private Integer customerID;
    //营销方案ID
    private Integer planID;
    //修改内容
    private Integer revisionContent;
    //修改状态
    private Integer revisionState;
    //最后修改时间
    private String updateTime;

    public String getFlagID() {
        return flagID;
    }

    public void setFlagID(String flagID) {
        this.flagID = flagID;
    }

    public Integer getPlanID() {
        return planID;
    }

    public void setPlanID(Integer planID) {
        this.planID = planID;
    }

    public Integer getRevisionContent() {
        return revisionContent;
    }

    public void setRevisionContent(Integer revisionContent) {
        this.revisionContent = revisionContent;
    }

    public Integer getRevisionState() {
        return revisionState;
    }

    public void setRevisionState(Integer revisionState) {
        this.revisionState = revisionState;
    }

    public String getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(String updateTime) {
        this.updateTime = updateTime;
    }
}
