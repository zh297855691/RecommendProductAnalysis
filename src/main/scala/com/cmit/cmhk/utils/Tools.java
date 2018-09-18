package com.cmit.cmhk.utils;

import com.cmit.cmhk.entity.*;
import com.cmit.cmhk.hbase.HBaseUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.concurrent.ConcurrentHashMap;

public class Tools {

    private static final Logger logger = LoggerFactory.getLogger(Tools.class.getName());

    /**
     * 获取mysql表MNG_PRA_MAPPING_CARRIER中漫游国与国家代码关系
     * @param conn
     */
    public static ConcurrentHashMap<String,String> getRoamingCountryCode(Connection conn) {

        String sql = "select ROAMING_COUNTRY_CODE,CARRIER_COUNTRY_CODE from MNG_PRA_MAPPING_CARRIER";
        ConcurrentHashMap<String,String> map = new ConcurrentHashMap();
        try {
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(sql);
            while (rs.next()) {
                //漫游国代码 -> 运营商国
               map.put(rs.getString("ROAMING_COUNTRY_CODE"),rs.getString("CARRIER_COUNTRY_CODE"));
            }
        } catch (SQLException e) {
            logger.error("获取表：ROAMING_COUNTRY_CODE 异常：" + e);
        }
        return map;
    }

    /**
     * 获取mysql表DC_MRS_IDD中长途区号与国家代码关系
     * @param conn
     */
    public static ConcurrentHashMap<String,String> getCountryCode(Connection conn) {

        String sql = "select IDD_CD,COUNTRY_CODE from DC_MRS_IDD";
        ConcurrentHashMap<String,String> map = new ConcurrentHashMap();
        try {
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(sql);
            while (rs.next()) {
                //关口号 -> 国家代码
                map.put(rs.getString("IDD_CD"),rs.getString("COUNTRY_CODE"));
            }
        } catch (SQLException e) {
            logger.error("获取表：DC_MRS_IDD 异常：" + e);
        }

        return map;
    }

    /**
     * 判断用户是否为白名单用户，true为不是白名单用户，false为白名单用户
     * @param conn
     * @return
     */
    public static ConcurrentHashMap<String,String> isOVL(Connection conn) {

        String sql = "select * from DC_MRS_OVL";
        ConcurrentHashMap<String,String> map = new ConcurrentHashMap<>();
        try {
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(sql);
            while(rs.next()) {
                map.put(rs.getString("MSISDN")+rs.getString("TYPE"),rs.getString("TYPE"));
            }
        } catch (SQLException e) {
            logger.error("获取表：DC_MRS_OVL 异常：" + e);
        }

        return map;
    }

    /**
     * 判断用户是否为后付费用户且生效中，true为是，false为不是
     * @param phone
     * @param hBaseUtils
     * @return
     */
    public static String isPostpay(String phone, HBaseUtils hBaseUtils) throws Exception {

        String flag = hBaseUtils.getRecordByRowkey(phone.substring(3));
        return flag;

    }



    /**
     * 根据用户手机号码及HBASEUtils对象，获取用户选择的语言
     * @param phone
     * @param hbaseUtils
     * @return
     */
    public static int getLanguage(String phone,HBaseUtils hbaseUtils) {
        int result = hbaseUtils.getUserLanguage(phone);
        return result;
    }

    /**
     * 加载用户标签配置表
     * @param conn
     * @return
     */
    public static ConcurrentHashMap<Integer,UserLabel> getUserLabel(String customerID,Connection conn) {

        String sql = "select LABEL_KEY,LABEL_ID,CUSTOMER_ID,EFFECT_TM,EXPIRE_TM,CONDITION_RESULT from " +
                "MNG_USER_LABEL_INFO where CUSTOMER_ID=\'" + customerID + "\'";
        ConcurrentHashMap<Integer,UserLabel> map = new ConcurrentHashMap<>();
        try {
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(sql);
            while (rs.next()) {
                UserLabel ul = new UserLabel();
                ul.setLabelKey(rs.getInt("LABEL_KEY"));
                ul.setLabelID(rs.getString("LABEL_ID"));
                ul.setCustomerID(rs.getString("CUSTOMER_ID"));
                ul.setEffectTM(rs.getString("EFFECT_TM"));
                ul.setExpireTM(rs.getString("EXPIRE_TM"));
                ul.setConditionResult(rs.getString("CONDITION_RESULT"));
                map.put(rs.getInt("LABEL_KEY"),ul);
            }
        } catch (Exception e) {
            logger.error("加载表：MNG_USER_LABEL_INFO 异常：" + e);
        }

        return map;
    }

    /**
     * 加载营销产品数据表
     * @param customerID
     * @param conn
     * @return
     */
    public static ConcurrentHashMap<Integer,ProductInfo> getProductInfo(String customerID,Connection conn) {

        String sql = "select PRODUCT_KEY,PRODUCT_ID,CUSTOMER_ID,PRODUCT_NAME,PRODUCT_NAME_EN,PRODUCT_TYPE,PRODUCT_PRICE,PRODUCT_DAYS," +
                "PRODUCT_SMS_CD,PRODUCT_USSD_CD,EFFECT_TM,EXPIRE_TM from MNG_MARKET_PRODUCT_INFO where CUSTOMER_ID=\'" + customerID + "\'";
        ConcurrentHashMap<Integer,ProductInfo> map = new ConcurrentHashMap<>();
        try {
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(sql);
            while (rs.next()) {
                ProductInfo pi = new ProductInfo();
                pi.setProductKey(rs.getInt("PRODUCT_KEY"));
                pi.setProductID(rs.getString("PRODUCT_ID"));
                pi.setCustomerID(rs.getString("CUSTOMER_ID"));
                pi.setProductName(rs.getString("PRODUCT_NAME"));
                pi.setProductNameEN(rs.getString("PRODUCT_NAME_EN"));
                pi.setProductType(rs.getString("PRODUCT_TYPE"));
                pi.setProductPrice(rs.getString("PRODUCT_PRICE"));
                pi.setProductDays(rs.getString("PRODUCT_DAYS"));
                pi.setProductSMSCD(rs.getString("PRODUCT_SMS_CD"));
                pi.setProductUSSDCD(rs.getString("PRODUCT_USSD_CD"));
                pi.setEffectTM(rs.getString("EFFECT_TM"));
                pi.setExpireTM(rs.getString("EXPIRE_TM"));
                map.put(rs.getInt("PRODUCT_KEY"),pi);
            }
        } catch (Exception e) {
            logger.error("加载表：MNG_MARKET_PRODUCT_INFO 异常：" + e);
        }
        return map;
    }

    /**
     * 加载营销方案问候短信模板数据表
     * @param conn
     * @return
     */
    public static ConcurrentHashMap<String,String> getPlanSMSInfo(Connection conn) {

        String sql = "select * from MNG_PLAN_SMS_INFO";
        ConcurrentHashMap<String,String> map = new ConcurrentHashMap<>();
        try {
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(sql);
            while (rs.next()) {
                map.put(rs.getString("PLAN_ID") + rs.getString("PLAN_SMS_LANG"),rs.getString("PLAN_SMS_CONT"));
            }
        } catch (Exception e) {
            logger.error("加载表：MNG_PLAN_SMS_INFO 异常：" + e);
        }

        return map;
    }

    /**
     * 加载营销产品覆盖国家数据表
     * @param conn
     * @return
     */
    public static ConcurrentHashMap<String,ArrayList<String>> getProductCountryByMCC(Connection conn) {

        ConcurrentHashMap<String,ArrayList<String>> pclMap = getCountryCodeByMCC(conn);
        String sql = "select * from DC_MRS_PCL";
        ConcurrentHashMap<String,ArrayList<String>> map = new ConcurrentHashMap<>();
        try {
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(sql);
            while (rs.next()) {
                String productID = rs.getString("PRODUCT_ID");
                String mcc = rs.getString("MCC");
                if(mcc !=null && productID != null && !pclMap.isEmpty()) {
                    if(pclMap.containsKey(mcc)) {
                        if(map.containsKey(productID)) {
                            map.get(productID).addAll(pclMap.get(mcc)); //合并
                            ArrayList<String> newList = new ArrayList<>(new LinkedHashSet<>(map.get(productID))); //去重
                            map.put(productID,newList);
                        } else {
                            map.put(productID,pclMap.get(mcc));
                        }
                    }
                }

            }

        } catch (Exception e) {
            System.out.println(e);
//            logger.error("加载表：MNG_PRODUCT_COUNTRY_INFO 异常：" + e);
        }

        return map;
    }
    public static ConcurrentHashMap<Integer,ArrayList<String>> getProductCountry(Connection conn) {

        String sql = "select * from MNG_PRODUCT_COUNTRY_INFO";
        ConcurrentHashMap<Integer,ArrayList<String>> map = new ConcurrentHashMap<>();
        try {
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(sql);
            while (rs.next()) {
                Integer productKey = rs.getInt("PRODUCT_KEY");
                if(map.containsKey(productKey)) {
                    map.get(productKey).add(rs.getString("COUNTRY_CD"));
                } else {
                    ArrayList<String> al = new ArrayList<String>();
                    al.add(rs.getString("COUNTRY_CD"));
                    map.put(productKey,al);
                }
            }
        } catch (Exception e) {
            logger.error("加载表：MNG_PRODUCT_COUNTRY_INFO 异常: " + e);
        }

        return map;
    }

    /**
     * 加载营销产品营销短信模板数据表
     * @param conn
     * @return
     */
    public static ConcurrentHashMap<String,String> getProductSMSInfo(Connection conn) {

        String sql = "select * from MNG_PRODUCT_SMS_INFO";
        ConcurrentHashMap<String,String> map = new ConcurrentHashMap<>();
        try {
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(sql);
            while (rs.next()) {
                map.put(rs.getString("PRODUCT_KEY") + rs.getString("PRODUCT_SMS_LANG"),rs.getString("PRODUCT_SMS_CONT"));
            }
        } catch (Exception e) {
            logger.error("加载表：MNG_PRODUCT_SMS_INFO 异常：" + e);
        }

        return map;
    }

    public static void SaveSMS(SmsProfile smsProfile,Connection conn) {

        String sql = "insert into MNG_SMS_IMSI_INFO(PLAN_ID,CUSTOMER_ID,MSISDN,COUNTRY_CD,SEND_NUM," +
                "CRE_TIME,PRODUCT_ID_SET,PRODUCT_NUM,FLAG,SMS_CONTENT,SEND_LANGUAGE) values(?,?,?,?,?,?,?,?,?,?,?)";
        PreparedStatement ps = null;
        try {
            conn.setAutoCommit(false);
            ps = (PreparedStatement)conn.prepareStatement(sql);
            ps.setInt(1,smsProfile.getPlanID()); //营销方案ID
            ps.setString(2,smsProfile.getCustomerID()); //客户ID
            ps.setString(3,smsProfile.getMsisdn()); //手机号码
            ps.setString(4,smsProfile.getCountryCode()); //国家代码
            ps.setInt(5,1); //第几次发送激活短信
            ps.setTimestamp(6,new java.sql.Timestamp(System.currentTimeMillis())); //记录创建时间
            ps.setString(7,smsProfile.getProductIDSet()); //推荐产品ID集合
            ps.setInt(8,smsProfile.getProductNum()); //推荐产品个数
            ps.setString(9,smsProfile.getFlag()); //触发标志，默认为1
            ps.setString(10,smsProfile.getSmsContent()); //短信内容
            ps.setString(11,smsProfile.getSendLanguage()); //短信语种

            ps.executeUpdate();
            conn.commit();
        }catch (Exception e) {
            logger.error("插入营销短信待发表：MNG_SMS_IMSI_INFO 异常：" + e);
        } finally {
            try {
                if(ps != null) {
                    ps.close();
                }
            } catch (SQLException e) {
                logger.error("关闭表：MNG_SMS_IMSI_INFO 异常：" + e);
            }
        }
    }

    public static ConcurrentHashMap<String,ArrayList<String>> getCountryCodeByMCC(Connection conn) {

        String sql = "select IMSI,COUNTRY_CODE from DC_MRS_CARRIER";
        ConcurrentHashMap<String,ArrayList<String>> map = new ConcurrentHashMap<String,ArrayList<String>>();
        try {
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(sql);
            while (rs.next()) {
                String mcc = rs.getString("IMSI").substring(0,3);
                if(map.containsKey(mcc)) {
                    map.get(mcc).add(rs.getString("COUNTRY_CODE"));
                } else {
                    ArrayList<String> al = new ArrayList<String>();
                    al.add(rs.getString("COUNTRY_CODE"));
                    map.put(mcc,al);
                }
            }
        } catch (Exception e) {
            logger.error("加载表：DC_MRS_CARRIER 异常：" + e);
        }

        return map;
    }

    /**
     * 返回给定ProductID绑定有效期内的ProductKey
     * @param pis
     * @param productID
     * @param time
     * @return
     */
    public static Integer getProductKey(ConcurrentHashMap<Integer, ProductInfo> pis,String productID,String time) {

        Integer productKey = 0;
        for(Integer p : pis.keySet()) {
            ProductInfo pi = pis.get(p);
            //productID绑定的ProductKey且在有效期内
            if(productID.equals(pi.getProductID()) && time.compareTo(Utils.getFormatTime2(pi.getEffectTM())) >= 0 && time.compareTo(Utils.getFormatTime2(pi.getExpireTM())) <=0) {
                productKey = p;
                break;//找到跳出循环
            }
        }
        return productKey;
    }

    /**
     * 返回给定LabelID绑定有效期内的LabelKey
     * @param uls
     * @param userLabelID
     * @param time
     * @return
     */
    public static Integer getLabelKey(ConcurrentHashMap<Integer, UserLabel> uls,String userLabelID,String time) {

        Integer labelKey = 0;
        for(Integer p : uls.keySet()) {
            UserLabel ul = uls.get(p);
            //labelID绑定的LabelKey且在有效期内
            if(userLabelID.equals(ul.getLabelID()) && time.compareTo(Utils.getFormatTime2(ul.getEffectTM())) >= 0 && time.compareTo(Utils.getFormatTime2(ul.getExpireTM())) <=0) {
                labelKey = p;
                break;//找到跳出循环
            }
        }
        return labelKey;
    }

    /**
     * 加国家中英文名称
     * @param conn
     * @return
     */
    public static ConcurrentHashMap<String, String> getCountryName(Connection conn) {

        String sql = "select COUNTRY_CD,COUNTRY_NAME,COUNTRY_NAME_EN from MNG_BASE_COUNTRY";
        ConcurrentHashMap<String, String> map = new ConcurrentHashMap<String, String>();
        try {
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(sql);
            while (rs.next()) {
                map.put(rs.getString("COUNTRY_CD") + "01",rs.getString("COUNTRY_NAME_EN"));
                map.put(rs.getString("COUNTRY_CD") + "02",rs.getString("COUNTRY_NAME"));
            }
        } catch (Exception e) {
            logger.error("加载表：MNG_BASE_COUNTRY 异常：" + e);
        }

        return map;
    }

    /**
     * 校验用户短信推送频率
     * @param conn
     * @param msisdn
     * @return
     */
    public static boolean checkIsPlanDay(Connection conn,String msisdn,int luday,String countryCode) {

        String sql = "select count(1) AS RecordCount from MNG_SMS_IMSI_RESULT where COUNTRY_CD=\'"+ countryCode  + "\' and MSISDN=\'"+msisdn+"\' and SEND_TIME >= DATE_SUB(NOW(),INTERVAL "+ luday +" DAY)";
        boolean flag = true;
        try {
            long recordcount = 0l;
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(sql);
            while (rs.next()) {
                recordcount = rs.getLong("RecordCount");
                System.out.println("发送条数为："+recordcount);
                if(recordcount >= 1) {
                    flag = false;
                }
            }
        } catch (Exception e) {
            logger.error("查询表：MNG_SMS_IMSI_RESULT 异常：" + e);
        }

        return flag;

    }


    /**
     * 判断当前用户当天是否已推送产品10040或10041，若有返回false，默认返回true
     * @param conn
     * @param msisdn
     * @return
     */
    public static boolean checkIsRight(Connection conn,String msisdn) {

        String sql = "select PRODUCT_ID_SET from MNG_SMS_IMSI_RESULT where MSISDN=\'" + msisdn + "\' and date_format(CRE_TIME,'%y-%m-%d')=date_format(NOW(),'%y-%m-%d')";
        String checkInfoSql = "select PRODUCT_ID_SET from MNG_SMS_IMSI_INFO where MSISDN = \'"  + msisdn + "\' and date_format(CRE_TIME,'%y-%m-%d')=date_format(NOW(),'%y-%m-%d')";
        boolean flag = true;
        try {
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(sql);
            while (rs.next()) {
                String[] record = rs.getString("PRODUCT_ID_SET").split("#",-1);
                for(String r : record) {
                    if("10040".equals(r) || "10041".equals(r)) {
                        flag = false;
                    }
                }
            }
            rs = st.executeQuery(checkInfoSql);
            while(rs.next()){
                String[] record = rs.getString("PRODUCT_ID_SET").split("#",-1);
                for(String r : record) {
                    if("10040".equals(r) || "10041".equals(r)) {
                        flag = false;
                    }
                }
            }
        } catch (Exception e) {
            logger.error("查询表：MNG_SMS_IMSI_RESULT和MNG_SMS_IMSI_INFO 异常：" + e);
        }

        return flag;

    }

}
