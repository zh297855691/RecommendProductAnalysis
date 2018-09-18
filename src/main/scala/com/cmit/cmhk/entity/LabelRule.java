package com.cmit.cmhk.entity;

import java.util.List;

public class LabelRule {

    /**
     * 是否为单个条件标志，true：单个条件，false：组合规则
     */
    private boolean singleRule;
    /**
     * 规则条件，取值为and或or
     */
    private String logic;
    /**
     * 组合规则列表
     */
    private List<LabelRule> ruleList;
    /**
     * 帅选条件，取值为：>,<,>=,<=,!=
     */
    private String rule;
    /**
     * 属性值
     */
    private String attribute;
    /**
     * 值
     */
    private String value;

    /**
     * 产品之间互斥关系
     * 0-无,1-强互斥,2-弱互斥
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
