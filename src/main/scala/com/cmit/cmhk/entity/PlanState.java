package com.cmit.cmhk.entity;

public class PlanState {
    //Ӫ������ID+�޸�����
    private String flagID;
    //�ͻ�ID
//    private Integer customerID;
    //Ӫ������ID
    private Integer planID;
    //�޸�����
    private Integer revisionContent;
    //�޸�״̬
    private Integer revisionState;
    //����޸�ʱ��
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
