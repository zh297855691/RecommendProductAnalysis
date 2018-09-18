package com.cmit.cmhk.entity;

import java.util.List;

public class LabelRule {

    /**
     * �Ƿ�Ϊ����������־��true������������false����Ϲ���
     */
    private boolean singleRule;
    /**
     * ����������ȡֵΪand��or
     */
    private String logic;
    /**
     * ��Ϲ����б�
     */
    private List<LabelRule> ruleList;
    /**
     * ˧ѡ������ȡֵΪ��>,<,>=,<=,!=
     */
    private String rule;
    /**
     * ����ֵ
     */
    private String attribute;
    /**
     * ֵ
     */
    private String value;

    /**
     * ��Ʒ֮�以���ϵ
     * 0-��,1-ǿ����,2-������
     * @return
     */
    private String instructions;

    public String getInstructions() {
        return instructions;
    }

    public void setInstructions(String instructions) {
        this.instructions = instructions;
    }

    public boolean isSingleRule() {
        return singleRule;
    }

    public void setSingleRule(boolean singleRule) {
        this.singleRule = singleRule;
    }

    public String getLogic() {
        return logic;
    }

    public void setLogic(String logic) {
        this.logic = logic;
    }

    public List<LabelRule> getRuleList() {
        return ruleList;
    }

    public void setRuleList(List<LabelRule> ruleList) {
        this.ruleList = ruleList;
    }

    public String getRule() {
        return rule;
    }

    public void setRule(String rule) {
        this.rule = rule;
    }

    public String getAttribute() {
        return attribute;
    }

    public void setAttribute(String attribute) {
        this.attribute = attribute;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String toString() {
        String result = "";
        return result = this.getAttribute() + ":" + this.getRuleList() + ":" + this.getRule() + ":" + this.getAttribute()
                + ":" + this.getLogic() + ":" + this.getInstructions();
    }
}
