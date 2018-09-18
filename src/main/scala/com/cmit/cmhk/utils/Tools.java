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
     * ��ȡmysql��MNG_PRA_MAPPING_CARRIER�����ι�����Ҵ����ϵ
     * @param conn
     */
    public static ConcurrentHashMap<String,String> getRoamingCountryCode(Connection conn) {

        String sql = "select ROAMING_COUNTRY_CODE,CARRIER_COUNTRY_CODE from MNG_PRA_MAPPING_CARRIER";
        ConcurrentHashMap<String,String> map = new ConcurrentHashMap();
        try {
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(sql);
            while (rs.next()) {
                //���ι����� -> ��Ӫ�̹�
               map.put(rs.getString("ROAMING_COUNTRY_CODE"),rs.getString("CARRIER_COUNTRY_CODE"));
            }
        } catch (SQLException e) {
            logger.error("��ȡ��ROAMING_COUNTRY_CODE �쳣��" + e);
        }
        return map;
    }

    /**
     * ��ȡmysql��DC_MRS_IDD�г�;��������Ҵ����ϵ
     * @param conn
     */
    public static ConcurrentHashMap<String,String> getCountryCode(Connection conn) {

        String sql = "select IDD_CD,COUNTRY_CODE from DC_MRS_IDD";
        ConcurrentHashMap<String,String> map = new ConcurrentHashMap();
        try {
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(sql);
            while (rs.next()) {
                //�ؿں� -> ���Ҵ���
                map.put(rs.getString("IDD_CD"),rs.getString("COUNTRY_CODE"));
            }
        } catch (SQLException e) {
            logger.error("��ȡ��DC_MRS_IDD �쳣��" + e);
        }

        return map;
    }

    /**
     * �ж��û��Ƿ�Ϊ�������û���trueΪ���ǰ������û���falseΪ�������û�
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
            logger.error("��ȡ��DC_MRS_OVL �쳣��" + e);
        }

        return map;
    }

    /**
     * �ж��û��Ƿ�Ϊ�󸶷��û�����Ч�У�trueΪ�ǣ�falseΪ����
     * @param phone
     * @param hBaseUtils
     * @return
     */
    public static String isPostpay(String phone, HBaseUtils hBaseUtils) throws Exception {

        String flag = hBaseUtils.getRecordByRowkey(phone.substring(3));
        return flag;

    }



    /**
     * �����û��ֻ����뼰HBASEUtils���󣬻�ȡ�û�ѡ�������
     * @param phone
     * @param hbaseUtils
     * @return
     */
    public static int getLanguage(String phone,HBaseUtils hbaseUtils) {
        int result = hbaseUtils.getUserLanguage(phone);
        return result;
    }

    /**
     * �����û���ǩ���ñ�
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
            logger.error("���ر�MNG_USER_LABEL_INFO �쳣��" + e);
        }

        return map;
    }

    /**
     * ����Ӫ����Ʒ���ݱ�
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
            logger.error("���ر�MNG_MARKET_PRODUCT_INFO �쳣��" + e);
        }
        return map;
    }

    /**
     * ����Ӫ�������ʺ����ģ�����ݱ�
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
            logger.error("���ر�MNG_PLAN_SMS_INFO �쳣��" + e);
        }

        return map;
    }

    /**
     * ����Ӫ����Ʒ���ǹ������ݱ�
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
                            map.get(productID).addAll(pclMap.get(mcc)); //�ϲ�
                            ArrayList<String> newList = new ArrayList<>(new LinkedHashSet<>(map.get(productID))); //ȥ��
                            map.put(productID,newList);
                        } else {
                            map.put(productID,pclMap.get(mcc));
                        }
                    }
                }

            }

        } catch (Exception e) {
            System.out.println(e);
//            logger.error("���ر�MNG_PRODUCT_COUNTRY_INFO �쳣��" + e);
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
            logger.error("���ر�MNG_PRODUCT_COUNTRY_INFO �쳣: " + e);
        }

        return map;
    }

    /**
     * ����Ӫ����ƷӪ������ģ�����ݱ�
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
            logger.error("���ر�MNG_PRODUCT_SMS_INFO �쳣��" + e);
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
            ps.setInt(1,smsProfile.getPlanID()); //Ӫ������ID
            ps.setString(2,smsProfile.getCustomerID()); //�ͻ�ID
            ps.setString(3,smsProfile.getMsisdn()); //�ֻ�����
            ps.setString(4,smsProfile.getCountryCode()); //���Ҵ���
            ps.setInt(5,1); //�ڼ��η��ͼ������
            ps.setTimestamp(6,new java.sql.Timestamp(System.currentTimeMillis())); //��¼����ʱ��
            ps.setString(7,smsProfile.getProductIDSet()); //�Ƽ���ƷID����
            ps.setInt(8,smsProfile.getProductNum()); //�Ƽ���Ʒ����
            ps.setString(9,smsProfile.getFlag()); //������־��Ĭ��Ϊ1
            ps.setString(10,smsProfile.getSmsContent()); //��������
            ps.setString(11,smsProfile.getSendLanguage()); //��������

            ps.executeUpdate();
            conn.commit();
        }catch (Exception e) {
            logger.error("����Ӫ�����Ŵ�����MNG_SMS_IMSI_INFO �쳣��" + e);
        } finally {
            try {
                if(ps != null) {
                    ps.close();
                }
            } catch (SQLException e) {
                logger.error("�رձ�MNG_SMS_IMSI_INFO �쳣��" + e);
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
            logger.error("���ر�DC_MRS_CARRIER �쳣��" + e);
        }

        return map;
    }

    /**
     * ���ظ���ProductID����Ч���ڵ�ProductKey
     * @param pis
     * @param productID
     * @param time
     * @return
     */
    public static Integer getProductKey(ConcurrentHashMap<Integer, ProductInfo> pis,String productID,String time) {

        Integer productKey = 0;
        for(Integer p : pis.keySet()) {
            ProductInfo pi = pis.get(p);
            //productID�󶨵�ProductKey������Ч����
            if(productID.equals(pi.getProductID()) && time.compareTo(Utils.getFormatTime2(pi.getEffectTM())) >= 0 && time.compareTo(Utils.getFormatTime2(pi.getExpireTM())) <=0) {
                productKey = p;
                break;//�ҵ�����ѭ��
            }
        }
        return productKey;
    }

    /**
     * ���ظ���LabelID����Ч���ڵ�LabelKey
     * @param uls
     * @param userLabelID
     * @param time
     * @return
     */
    public static Integer getLabelKey(ConcurrentHashMap<Integer, UserLabel> uls,String userLabelID,String time) {

        Integer labelKey = 0;
        for(Integer p : uls.keySet()) {
            UserLabel ul = uls.get(p);
            //labelID�󶨵�LabelKey������Ч����
            if(userLabelID.equals(ul.getLabelID()) && time.compareTo(Utils.getFormatTime2(ul.getEffectTM())) >= 0 && time.compareTo(Utils.getFormatTime2(ul.getExpireTM())) <=0) {
                labelKey = p;
                break;//�ҵ�����ѭ��
            }
        }
        return labelKey;
    }

    /**
     * �ӹ�����Ӣ������
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
            logger.error("���ر�MNG_BASE_COUNTRY �쳣��" + e);
        }

        return map;
    }

    /**
     * У���û���������Ƶ��
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
                System.out.println("��������Ϊ��"+recordcount);
                if(recordcount >= 1) {
                    flag = false;
                }
            }
        } catch (Exception e) {
            logger.error("��ѯ��MNG_SMS_IMSI_RESULT �쳣��" + e);
        }

        return flag;

    }


    /**
     * �жϵ�ǰ�û������Ƿ������Ͳ�Ʒ10040��10041�����з���false��Ĭ�Ϸ���true
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
            logger.error("��ѯ��MNG_SMS_IMSI_RESULT��MNG_SMS_IMSI_INFO �쳣��" + e);
        }

        return flag;

    }

}
