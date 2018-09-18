package com.cmit.cmhk.utils

import java.sql.{Connection, ResultSet, Statement}
import java.util
import java.util.ArrayList
import java.util.concurrent.ConcurrentHashMap

import com.cmit.cmhk.entity._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

  /**
  * Created by fengzhihong on 2018/03/16.
  */
class LoadMySQLData extends Serializable {

  private val serialVersionUID = 6351075102138547638L

    val log: Logger = LoggerFactory.getLogger(LoadMySQLData.getClass)

  /**
    * Ӫ������
    */
  var marketPlan: ArrayBuffer[MarketPlan] = new ArrayBuffer[MarketPlan]()
  /**
    * Ӫ����Ʒ
    */
  var productInfo: ConcurrentHashMap[Integer, ProductInfo] = new ConcurrentHashMap[Integer,ProductInfo]()
  /**
    * �û���ǩ
    */
  var userLabel: ConcurrentHashMap[Integer, UserLabel] = new ConcurrentHashMap[Integer,UserLabel]()
  /**
    * Ӫ����������ģ��
    */
  var planSMSInfo: ConcurrentHashMap[String,String] = new ConcurrentHashMap[String,String]()
  /**
    * Ӫ����Ʒ����ģ��
    */
  var productSMSInfo: ConcurrentHashMap[String,String] = new ConcurrentHashMap[String,String]()
  /**
    * �����Ƽ���Ʒ�󶨹�ϵ
    */
  var planProduct: ArrayBuffer[PlanProduct] = new ArrayBuffer[PlanProduct]()
  /**
    * Ӫ����Ʒ���ǹ���
    */
  var productCountry: ConcurrentHashMap[Integer, util.ArrayList[String]] = new ConcurrentHashMap[Integer, util.ArrayList[String]]()
  /**
    * ������Ӣ������
    */
  var countryName: ConcurrentHashMap[String, String] = new ConcurrentHashMap[String, String]()

  def initValue(customerID: String,conn: Connection): Unit = {

    log.debug("spark�㲥��ȡmarketPlan��planSMSInfo��planProduct��productInfo��productCountry��productCountry��productSMSInfo��countryName��userLabel����")
    marketPlan = LoadMySQLData.getPlans(customerID,conn)
    planSMSInfo = Tools.getPlanSMSInfo(conn)
    planProduct = LoadMySQLData.getPlanProduct(conn)

    productInfo = Tools.getProductInfo(customerID,conn)
    productCountry = Tools.getProductCountry(conn)
    productSMSInfo = Tools.getProductSMSInfo(conn)
    countryName = Tools.getCountryName(conn)

    userLabel = Tools.getUserLabel(customerID,conn)


  }

  def initValueByStates(customerID: String, flag: mutable.Map[Int,Boolean], conn: Connection): Unit = {
    var message = ""
    for(f <- flag.keys) {
      f match {
        case 1 => {
          if(flag.get(f).get) { //�޸��û���ǩ�������
            userLabel = Tools.getUserLabel(customerID, conn) //�û���ǩ���ñ�
            message = "userLabel"
          }
        }
        case 2 => {
          if(flag.get(f).get) { //�޸�Ӫ����Ʒ�������
            productInfo = Tools.getProductInfo(customerID,conn) //��Ʒ��Ϣ
            productCountry = Tools.getProductCountry(conn) //��Ʒ���ǹ���
            productSMSInfo = Tools.getProductSMSInfo(conn) //��ƷӪ������
            countryName = Tools.getCountryName(conn)
            message = "productInfo,productCountry,productSMSInfo,countryName"
          }
        }
        case 3 => {
          if(flag.get(f).get) { //�޸ķ����Ƽ���Ʒ���ݱ�Ӫ�������������
            planProduct = LoadMySQLData.getPlanProduct(conn) //�����Ƽ���Ʒ���ݱ�
            marketPlan = LoadMySQLData.getPlans(customerID,conn) //Ӫ������
            planSMSInfo = Tools.getPlanSMSInfo(conn) //Ӫ����������ģ��
            message = "planProduct,marketPlan,planSMSInfo"
          }
        }
      }
      log.debug("spark�㲥����" + message + "����")
    }

  }
}

object LoadMySQLData {

  var loadMySQLData: LoadMySQLData = _
  def getLoadMySQLData(): LoadMySQLData = {
    synchronized {
      if(loadMySQLData == null) {
        loadMySQLData = new LoadMySQLData
      }
    }
    loadMySQLData
  }

  /**
    * ���ط����Ƽ���Ʒ���ݱ�
    * @param conn
    * @return
    */
  def getPlanProduct(conn: Connection): ArrayBuffer[PlanProduct] = {
    val sql = "select * from MNG_PLAN_PRODUCT_INFO"
    val alpp = new ArrayBuffer[PlanProduct]
    try {
      val st = conn.createStatement
      val rs = st.executeQuery(sql)
      while (rs.next()) {
        val pp = new PlanProduct
        pp.setPlanID(rs.getInt("PLAN_ID"))
        pp.setProductID(rs.getString("PRODUCT_ID"))
        pp.setProductPriority(rs.getString("PRODUCT_PRIORITY"))
        pp.setLabelID(rs.getString("LABEL_ID"))
        pp.setSendLimit(rs.getString("SEND_LIMIT"))
        alpp += pp
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    alpp
  }

  /**
    * ���ݿͻ�ID����Ӫ���������ñ�
    * @param customerID
    * @param conn
    * @return
    */
  def getPlans(customerID: String, conn: Connection): ArrayBuffer[MarketPlan] = {
    val sql = "select PLAN_ID,CUSTOMER_ID,EFFECT_TM,EXPIRE_TM,PLAN_STATUS,PLAN_LU_DAY from MNG_MARKET_PLAN_INFO where CUSTOMER_ID=\'" + customerID + "\'"
    val list = new ArrayBuffer[MarketPlan]
    try {
      val st = conn.createStatement
      val rs = st.executeQuery(sql)
      while ( {
        rs.next
      }) {
        val mp = new MarketPlan
        mp.setPlanID(rs.getInt("PLAN_ID"))
        mp.setCustomerID(rs.getString("CUSTOMER_ID"))
        mp.setEffectTM(rs.getString("EFFECT_TM"))
        mp.setExpireTM(rs.getString("EXPIRE_TM"))
        mp.setPlanStatus(rs.getString("PLAN_STATUS"))
        mp.setPlanLUDay(rs.getString("PLAN_LU_DAY"))
        list += mp
      }
    } catch {
      case e: Exception =>
       e.printStackTrace()
    }
    list
  }

  /**
    * ��ȡ����״̬��
    * @param customerID
    * @param conn
    * @return
    */
  def getState(customerID: String, conn: Connection): ArrayBuffer[PlanState] = {

    val sql = "select FLAG_ID,PLAN_ID,REVISION_CONTENT,REVISION_STATE,UPDATE_TIME from MNG_PRA_PRODUCT_STATE where CUSTOMER_ID=\'" + customerID + "\'"
    val pp = new ArrayBuffer[PlanState]
    try {
      val st = conn.createStatement
      val rs = st.executeQuery(sql)
      while (rs.next()) {
        val ps = new PlanState
        ps.setFlagID(rs.getString("FLAG_ID"))
        ps.setRevisionContent(rs.getInt("REVISION_CONTENT"))
        ps.setRevisionState(rs.getInt("REVISION_STATE"))
        ps.setUpdateTime(rs.getString("UPDATE_TIME"))
        ps.setPlanID(rs.getInt("PLAN_ID"))
       pp += ps
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    pp
  }

}

