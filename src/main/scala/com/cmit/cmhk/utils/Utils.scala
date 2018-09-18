package com.cmit.cmhk.utils

import java.sql.{Connection, PreparedStatement}
import java.text.SimpleDateFormat
import java.util
import java.util.{ArrayList, Calendar}
import java.util.concurrent.ConcurrentHashMap

import com.cmit.cmhk.entity._
import com.cmit.cmhk.hbase.HBaseUtils
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.ObjectMapper
import javax.script.ScriptEngineManager
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ArrayBuffer


 /**
  * Created by fengzhihong on 2018/03/16.
  */

object Utils {

   val log: Logger = LoggerFactory.getLogger(Utils.getClass)
//   val dateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
//   val dateFormat1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//   val dateFormat2 = new SimpleDateFormat("yyyyMMdd")

   /**
    * ���ص�ǰʱ��ǰ���ٸ�Сʱ
    * @param hour
    * @return
    */
  def getBeforeHourTime(hour: Int) = {

    val calendar = Calendar.getInstance()
    calendar.set(Calendar.HOUR_OF_DAY, calendar.get(Calendar.HOUR_OF_DAY) - hour)
    val df = new SimpleDateFormat("yyyyMMddHHmmss")
    val result = df.format(calendar.getTime)
    result
  }

  /**
    * ��ʽ��ʱ���ʽΪyyyyMMddHHmmss
    * @param str
    * @return
    */
  def getFormatTime(str: String): String = {
    synchronized{
      val dateFormat2 = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
      val dateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
      val date = dateFormat2.parse(str)
      val last = dateFormat.format(date)
      last
    }
  }

  /**
    * ��ʽ��ʱ���ʽΪyyyyMMddHHmmss
    * @param str
    * @return
    */
  def getFormatTime2(str: String): String = {
    synchronized{
      val dateFormat2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val dateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
      val date = dateFormat2.parse(str)
      val last = dateFormat.format(date)
      last
    }
  }

  /**
    * ���ݹؿںŻ�ȡ���Ҵ���
    * @param str
    * @return
    */
  def getCountryCodeByADDR(str: String,countryCodeMap: ConcurrentHashMap[String,String],myCountryCodeMap: ConcurrentHashMap[String,String],hBaseUtils: HBaseUtils): Option[String] = {

    var countryCode = hBaseUtils.getCountryCodeByADDR(str)
    if(countryCode.isEmpty) { //��������޹ؿںŶ�Ӧ������Ӫ�̹����룬����Ҫ����
      val result = extractCountryCode(str,countryCodeMap)
      if(result.isDefined) {
        if(myCountryCodeMap.containsKey(result.get)) {
          countryCode = Some(myCountryCodeMap.get(result.get))
          //����HBase��mrs:RPA_MRS_MSC_COUNTRY_CODE(�ؿں�����Ӫ�̹��Ҵ����)
          hBaseUtils.saveCountryCode(str,countryCode.get)
        }
      }
    }
    countryCode
  }

  /**
    * ���ݹ���ؿں�ƥ�����ι��Ҵ���
    * @param str
    * @param map
    * @return
    */
  def extractCountryCode(str: String, map: ConcurrentHashMap[String, String]): Option[String] = {

    var length = str.length
    var flag = true
    var value: Option[String] = None
    while (length > 0 && flag) {
      if(map.containsKey(str.substring(0,length))) {
        value = Some(map.get(str.substring(0,length)))
        flag = false
      }
      length = length - 1
    }
    value
  }

   /**
     * �����߼���ں���
     * @param rowkey
     * @param hbaseUtils
     * @param customerID
     * @param loadMySQLData
     * @param ovlMap
     * @param record
     * @param conn
     */
  def processMarketingSMS(rowkey: String,hbaseUtils: HBaseUtils,customerID: String,loadMySQLData: LoadMySQLData,
                          ovlMap: ConcurrentHashMap[String,String],record: String,conn: Connection): Unit = {
    log.debug("����Ӫ���Ƽ��߼������ü�¼rowkey��" + rowkey + ",value:" + record)
    val dateFormat2 = new SimpleDateFormat("yyyyMMdd")
    val currentDate = dateFormat2.format(System.currentTimeMillis())
    val countryCode = record.split("#", -1)(2)
    //���Ϊ�ձ��û���ֱ�Ӳ������Ƽ�
    if(countryCode == "JPN"){
        return
    }

    val userRealTimeLabel = new UserRealTimeLabel()
    //��ȡ���û��ֻ����룺����û��ֻ�����11λ����������85��
    val phone = rowkey.trim.substring(15)
    //���ζ��ŷ���ʱ��
    val time = rowkey.substring(1,15)
    //�ж��û��Ƿ�Ϊ�������û�
    val ovl = ovlMap.containsKey(phone.substring(3)+"1") || ovlMap.containsKey(phone.substring(3)+"3")
    //�ж�90�����Ƿ��ж�����Ʒ��¼
    val orderRecord = ovlMap.containsKey(phone.substring(3)+"2")
    /**
      * 99999һ��ͻ�Ԥ����
      * 99998��ҵ�ͻ�Ԥ����
      */
    val postpay = Tools.isPostpay(phone,hbaseUtils)
    //��ȡ���û���ѡ�������
    val language = Tools.getLanguage(phone.substring(3),hbaseUtils)
    //���ص�ǰ�û�������Ʒ����Ϣ
    val productInfo = hbaseUtils.findOrderingInfo(phone)
    log.info("��ǰ�û�������Ч�Ĳ�Ʒ��Ϣ��" + productInfo)
    //��ſ�����Ҫ�Ƽ���ƷKey
    val recommendProducts = new ArrayBuffer[(Int,Int,String,String,Int)]()
    val NewrecommendProducts = new ArrayBuffer[(Int,Int,String,String,Int)]()
    var promap = Map[String,Int]()
    //�ǰ������Һ󸶷��Ҵ���ѡ�����Ե��û�����Ӫ������
    log.debug("��ǰ�û�" + phone+"�Ƿ�Ϊ�������û���" + ovl + ",Ԥ�����û����ͣ�" + postpay + ",�û�ѡ������ԣ�" + language + ",�û���ǰλ��:" + countryCode)
    //�û�ʵʱ��ǩ��&�û�λ�ø��±�
    userRealTimeLabel.setMsisdn(phone)
    userRealTimeLabel.setCountryCode(countryCode)
    userRealTimeLabel.setLuTime(time)
    userRealTimeLabel.setSource("WSMS")
    userRealTimeLabel.setIsPayList(postpay)
    userRealTimeLabel.setOrderProductID(productInfo.get._2)

    var rr: Map[String,UserRecommendRecord] = Map()

    if(!ovl && "99999".equals(postpay) && language != 0) {
      userRealTimeLabel.setIsWhiteList("0")
      log.debug("��ǰ�û�ʵʱ��ǩ��" + userRealTimeLabel)
      val mps: ArrayBuffer[MarketPlan] = loadMySQLData.marketPlan //Ӫ������
      val pis: ConcurrentHashMap[Integer, ProductInfo] = loadMySQLData.productInfo //Ӫ����Ʒ
      val uls: ConcurrentHashMap[Integer, UserLabel] = loadMySQLData.userLabel //�û���ǩ
      val pps: ArrayBuffer[PlanProduct] = loadMySQLData.planProduct //�����Ƽ���Ʒ�󶨹�ϵ
      val psi: ConcurrentHashMap[String,String] = loadMySQLData.planSMSInfo//Ӫ����������ģ��
      val pc: ConcurrentHashMap[Integer, util.ArrayList[String]] = loadMySQLData.productCountry //��Ʒ���ǹ���
      val ppi: ConcurrentHashMap[String,String] = loadMySQLData.productSMSInfo//Ӫ����Ʒ����ģ��
      val cn: ConcurrentHashMap[String,String] = loadMySQLData.countryName //������Ӣ������
      val dateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
      val currentTime = dateFormat.format(System.currentTimeMillis())
      /**
        * �����Ʒ���󣬸����û�90���ײ������Ⱥ
        */
      val productSet = List[String]("00040","00041","10040","10041","00011","00012","10011","10012")

      //��¼�����Ͳ�Ʒ��(90����˹���)
      var speacialSet = List[String]()
      var isDelete = 0
      //��Ӫ��������Ӧ������Ч������������
      for( mp: MarketPlan <- mps if getFormatTime2(mp.getEffectTM).compareTo(currentTime) <= 0
        && getFormatTime2(mp.getExpireTM).compareTo(currentTime) >= 0 && mp.getPlanStatus.equals("0")) {
        /**
          * �����û����λ�ӭ����ʱ�䣺
          * 1���жϸ÷����󶨱�ǩ����Ч���Ƿ񺭸��û����λ�ӭ���ŵ�ʱ��
          * 2���жϸ÷�����Ӫ����Ʒ����Ч���Ƿ񺭸��û����λ�ӭ���ŵ�ʱ��
          */
        for(pp: PlanProduct <- pps if pp.getPlanID == mp.getPlanID) {

          /**
            * ��ȡProductID����Ч���ڵ�ProductKey
            * ��ȡLabelID����Ч���ڵ�LabelKey
            */
          val productKey = Tools.getProductKey(pis,pp.getProductID,time)
          log.debug("���ݵ�ǰ����" + mp.getPlanID + "�Ĳ�ƷID" + pp.getProductID + "��luʱ��" + time + "��ȡ��productKey:" + productKey)
          val labelKey = Tools.getLabelKey(uls,pp.getLabelID,time)
          log.debug("���ݵ�ǰ����" + mp.getPlanID + "�Ĳ�ƷID" + pp.getLabelID + "��luʱ��" + time + "��ȡ��labelKey:" + labelKey)

          //00Ϊ�������Ʒ����Ʒ���ƣ��۸�������10ΪԤ�����Ʒ�����ű���
          var productKey00 = 0
          if(pp.getProductID.substring(0,2).equals("10")) {
            productKey00 = Tools.getProductKey(pis,"00" + pp.getProductID.substring(2),time)
          } else if(pp.getProductID.substring(0,2).equals("00") || pp.getProductID.substring(0,2).equals("01")) {
            productKey00 = productKey
          }


          var flag = false
          var products: List[String] = List()
          if(!"".equals(productInfo.get._2)) {
            products = productInfo.get._2.split("#",-1).toList
          }
//          log.debug("��ǰ�û�" + phone + "������ƷΪ��" + products)
          //�ж������Ͽ������͵Ĳ�Ʒ���ϣ�Ĭ�ϲ���
          log.debug("�жϱ�ǩ" + labelKey + "�Ƿ����ڸ��û���ǩ��")
          if(uls.containsKey(labelKey)) {
            log.debug("�û���ǩ������" + labelKey + "��ǩ�����б�ǩ�����Ƽ��ж�")
            flag = isPush(uls.get(labelKey).getConditionResult,products,countryCode,pc,pis,time)
            log.debug(phone + "�û���ǩ�߼��ж����ͽ����" + flag)
          }else{
            log.debug("�û���ǩ��������" + labelKey + "��ǩ")
          }
          //��ȡ��������
          var limit = 0l
          var isLimit: Boolean = false
//          log.info("��ƷID��" + pp.getProductID + "����������Ϊ��"+pp.getSendLimit)
          if("".equals(pp.getSendLimit)) {
            isLimit = true
            log.info("��ƷID��" + pp.getProductID + "�����������������Ƽ�")
          } else if("0".equals(pp.getSendLimit)) {
            isLimit = false
            log.info("��ƷID��" + pp.getProductID + "����������Ϊ0�������Ƽ�")
          } else {
            limit = pp.getSendLimit.toLong
            isLimit = hbaseUtils.productCountInsert(currentDate + pp.getProductID.substring(2),limit)
          }

          if(flag) {

            //��¼�û����͹���
            val urr = new UserRecommendRecord
            urr.setPlanID(pp.getPlanID.toString)
            urr.setMsisdn(phone)
            urr.setCountryCode(countryCode)
            urr.setLuTime(time)
            urr.setPri(pp.getProductPriority)
            urr.setProductID(pp.getProductID)
            rr += (pp.getProductID -> urr)

            //�õ�ǰ��Ʒ�����û����ڹ���ʱ�ſ����Ƽ�
            if(pc.containsKey(productKey) && pc.get(productKey).contains(countryCode)) { //�޳�
              log.info(phone+"��ƷID��" + pp.getProductID + "�����Ƽ���")
              if(isLimit) {
                speacialSet = speacialSet :+ pp.getProductID
                recommendProducts += ((productKey,productKey00,pp.getProductID,pis.get(productKey).getProductType,pp.getProductPriority.toInt))
              } else {
                log.info("��Ʒ��"+ pp.getProductID +",������������������ƣ�")
              }
            } else {
              log.info("��Ʒ��" + pp.getProductID + "δ���ǵ�ǰ���ҡ�" + countryCode +"��")
            }
          }
        }
        log.debug("��ǰ�û�" + phone + "�Ƽ���Ʒ����" + recommendProducts)
        /**
          * �޳��������߼��Ĳ�Ʒ
          */
        if((speacialSet.contains("10040") || speacialSet.contains("10041")) && (speacialSet.contains("10011") || speacialSet.contains("10012"))) {
          isDelete = 1 //��ʾͬʱ������10040��10041��10011��10012
        } else if ((!speacialSet.contains("10040") && !speacialSet.contains("10041")) && (speacialSet.contains("10011") || speacialSet.contains("10012"))) {
          isDelete = 2 //��ʾ����10011,10012
        } else if ((speacialSet.contains("10040") || speacialSet.contains("10041")) && (!speacialSet.contains("10011")  && !speacialSet.contains("10012"))) {
          isDelete = 3 //��ʾ����10041,10040
        }

        /**
          * ����90��Ĺ����߼�
          */
        log.debug("-------------����90�����-------------")
        for(record <- recommendProducts) {
          isDelete match {
            case 1 => {
              if(orderRecord) { //����90�충����¼������10040��10041
                record._3 match {
                  case "10011" => NewrecommendProducts += record
                  case "10012" => NewrecommendProducts += record
                  case _ => log.info("��ǰ�û���" + phone +"����90�충����¼�������ͣ�" + record._3)
                }
              } else { //����90�충����¼
                record._3 match {
                  case "10040" => NewrecommendProducts += record
                  case "10041" => NewrecommendProducts += record
                  case _ => log.info("��ǰ�û���" + phone +"������90�충����¼�������ͣ�" + record._3)
                }
              }
            }
            case 3 => {
              if(!orderRecord) { //������90�충����¼
                NewrecommendProducts += record
              } else {
                log.info("��ǰ�û���" + phone +"����90�충����¼�������ͣ�" + record._3)
              }
            }
            case _ => NewrecommendProducts += record
          }
        }
        log.debug("��ǰ�û����˺���Ƽ���Ʒ��Ϊ��" + NewrecommendProducts)
        /**
          * ��Ʒ���ȼ�����
          */
//        log.debug("------------���в�Ʒ���ȼ��ж�-----------------")
        for((productKey,productKey00,productID,productType,priority) <- NewrecommendProducts) {
          //����ÿ�ֲ�Ʒ���͵���С���ȼ�(1Ϊ������ȼ�)
          if(promap.contains(productType) && promap.get(productType).get > priority) {
            promap += (productType -> priority)
          } else {
            promap += (productType -> priority)
          }
        }

        /**
          * Ӫ���������ɴ�����
          * Ӫ���������ʺ����ģ������(�߼�Ϊһ��Ӫ����������һ��Ӫ������)
          */
        var smsContentHead = ("","")
        var productSmsInfo = ""
        var productSmsInfoList = List[(Int,String)]()
        if(language == 1 && psi.containsKey(mp.getPlanID + "02") && cn.containsKey(countryCode+"02")) { //���İ汾
          smsContentHead = parseHeadForChi(psi.get(mp.getPlanID + "02"),cn.get(countryCode+"02"))
        } else if (language == 2 && psi.containsKey(mp.getPlanID + "01") && cn.containsKey(countryCode + "01")) { //Ӣ�İ汾
          smsContentHead = parseHeadForEng(psi.get(mp.getPlanID + "01"),cn.get(countryCode + "01"))
        } else {
          log.warn("��ע�⡿��ǰӪ������δ�������Ļ�Ӣ��Ӫ������ģ�壬Ӫ��������" + mp.getPlanID)
        }

        var smsContentEndList = List[(Int,String)]()
        var productIDs = new StringBuilder
        //�������ȼ���������Ҫ�Ƽ��Ĳ�Ʒ�����жϸò�Ʒ�Ƿ񺭸ǵ�ǰ��Ӫ�̹�
        for((productKey,productKey00,productID,productType,priority) <- NewrecommendProducts if (promap.contains(productType) && priority == promap.get(productType).get)) {
          productIDs.append(productID + "#")
          language match {
            case 1 => {
              if(ppi.containsKey(productKey + "02") && pis.containsKey(productKey) && pis.containsKey(productKey00) && cn.containsKey(countryCode + "02")) {
                productSmsInfoList = productSmsInfoList :+ (pis.get(productKey00).getProductPrice.toInt -> pis.get(productKey00).getProductDays)
                smsContentEndList = smsContentEndList :+ (pis.get(productKey00).getProductPrice.toInt -> getSmsContentChi(ppi.get(productKey + "02"),
                  pis.get(productKey00).getProductName,pis.get(productKey00).getProductPrice,pis.get(productKey).getProductSMSCD,cn.get(countryCode + "02")))
              } else {
                log.warn("��ע�⡿��ƷID��" + productID + ",��ƷProductKey:" + productKey + " δ�������Ķ���ģ�壡")
              }
            }
            case 2 => {
              if(ppi.containsKey(productKey + "01") && pis.containsKey(productKey) && pis.containsKey(productKey00) && cn.containsKey(countryCode + "01")) {
                productSmsInfoList = productSmsInfoList :+ (pis.get(productKey00).getProductPrice.toInt -> pis.get(productKey00).getProductDays)
                smsContentEndList = smsContentEndList :+ (pis.get(productKey00).getProductPrice.toInt -> getSmsContentEng(ppi.get(productKey + "01"),
                  pis.get(productKey00).getProductNameEN,pis.get(productKey00).getProductPrice,pis.get(productKey).getProductSMSCD,cn.get(countryCode + "01")))
              } else {
                log.warn("��ע�⡿��ƷID��" + productID + ",��ƷProductKey:" + productKey +":" +productKey00 + " δ����Ӣ�Ķ���ģ�壡")
              }
            }
          }
        }
        var smsContentHeadFront = ""
        for(list <- productSmsInfoList.sorted) {
          language match {
            case 1 => {//����
              productSmsInfo = productSmsInfo + "HK$" + list._1 +"��" + list._2 + "�գ�" + "/"
            }
            case 2 => {//Ӣ��
              if(list._2.toInt > 1) {
                productSmsInfo = productSmsInfo + "HK$" + list._1 +"(" + list._2 + "days)" + "/"
              } else {
                productSmsInfo = productSmsInfo + "HK$" + list._1 +"(" + list._2 + "day)" + "/"
              }
            }
          }
        }
        var smsContentEnd = ""
        var sort = 1 //�����Ҫ�Ƽ���Ʒ���
        if(smsContentEndList.length == 1) {
          for(list <- smsContentEndList.sorted) {
            smsContentEnd = list._2 + "\n"
          }
        } else if(smsContentEndList.length > 1) {
          for(list <- smsContentEndList.sorted) {
            language match {
              case 1 => { //����
                smsContentEnd = smsContentEnd + sort + "." + list._2 + "\n"
              }
              case 2 => { //Ӣ��
                smsContentEnd = smsContentEnd + sort + ". " + list._2 + "\n"
              }
            }
            sort = sort + 1
          }
        }

        if(language == 1 && !"".equals(smsContentHead._1) && !"".equals(productSmsInfo)) { //����
          smsContentHeadFront = smsContentHead._1.substring(1,smsContentHead._1.indexOf("[")) +
            productSmsInfo.substring(0,productSmsInfo.length - 1) + smsContentHead._1.substring(smsContentHead._1.indexOf("]") + 1)
        } else if (language == 2 && !"".equals(smsContentHead._1) && !"".equals(productSmsInfo)) { //Ӣ��
          smsContentHeadFront = smsContentHead._1.substring(1,smsContentHead._1.indexOf("[")) +
            productSmsInfo.substring(0,productSmsInfo.length - 1) + smsContentHead._1.substring(smsContentHead._1.indexOf("]") + 1)
        }

        /**
          * ���ŷ�����
          */
        val smsMsg = new SmsProfile
        if(!smsContentHead._1.equals("") && !smsContentHead._2.equals("") && !smsContentEnd.equals("")) {
          smsMsg.setSendLanguage(language.toString)
          smsMsg.setMsisdn(phone)
          smsMsg.setCountryCode(countryCode)
          smsMsg.setCustomerID(customerID)
          smsMsg.setPlanID(mp.getPlanID)
          smsMsg.setSmsContent(smsContentHeadFront + "\n" + "\n" + smsContentEnd + "\n" +smsContentHead._2) //��������
          smsMsg.setFlag("1")//���λ�ӭ���Ŵ���
          smsMsg.setProductIDSet(productIDs.substring(0,productIDs.length - 1))
          smsMsg.setProductNum(productIDs.substring(0,productIDs.length - 1).split("#",-1).length)

          /**
            * ���ݵ�ǰ���λ�ӭ���ŷ���ʱ��֮��GGSN���������أ�����GGSN��¼ʱ���Ҵ��ڵ�ǰ��������ʱ��ļ�¼�����ң�
            */
          val isLeave = hbaseUtils.isInThisCountry(countryCode,phone,time)
          log.debug("------------ͨ��GGSN�����жϵ�ǰ�û�" + phone + "����λ�ã�" + isLeave)
          log.info(phone + " GGSN is Leave:" + isLeave)
          /**
            * ���أ��ж��Ƿ�Ϊ�������»�ӭ���ţ����¶��ż�¼ʱ�䣬���ң�
            */
          val isNewestSMS = hbaseUtils.isNewestSMS(phone,time)
          log.debug("-------------�жϵ�ǰ�û�" + phone + "��LUʱ��" + time + "�Ƿ�Ϊ���»�ӭ���ţ�" + isNewestSMS._1)
          log.info(phone + " SMS is Newest:" + isNewestSMS)

          /**
            * У���Ƿ�����LU��������
            */
//          val isPlanLUDay = Tools.checkIsPlanDay(conn,phone,mp.getPlanLUDay.toInt,countryCode)
          val (luTime,nowTime) = getLuTime(mp.getPlanLUDay.toInt)
          val isPlanLUDay: Boolean = hbaseUtils.checkIsPlanDay(phone,countryCode,luTime)
          log.debug("----------------У���Ƿ�����LU����������" + isPlanLUDay)

          /**
            * �������»�ӭ���ź�GGSN������ͬ�ж��û�������λ��
            */

          var location: Boolean = false
          if("".equals(isLeave._1) || "".equals(isLeave._2)) { //��GGSN��¼
            if(isNewestSMS._1) { //��ǰΪ���¶��ż�¼�����޴��ڵ�ǰ����ʱ���GGSN��¼
              location = true
            }
          } else { //��������GGSN
            if(isNewestSMS._1) { //��ǰΪ���¶��ż�¼
              if(countryCode.equals(isLeave._2)) { //�û���ǰ���Һ�GGSN����������ͬ
                location = true
              }
            } else if(isNewestSMS._2.compareTo(isLeave._1) < 0 && countryCode.equals(isLeave._2)) { //���¶���ʱ��С������GGSN����ʱ��
              if(isNewestSMS._3.equals(countryCode)) { //������¶��Ź����뵱ǰ���Ź�����ͬ����Ϊfalse
                location = false
//                location = true
              } else {
                location = true
//                location = false
              }
            }
          }
          log.debug("-----------�������»�ӭ���ź�GGSN������ͬ�жϵ�ǰ�û�" + phone + "�Ƿ����뿪��" + !location)
          log.info(phone + " �û�λ����Ϣ��"+location + "��15��LU�����߼���" + isPlanLUDay)

          /**
            * �������û��Ѿ�����10040��10041�����Ƽ�10011��10012
            */
          val prodcutArray = smsMsg.getProductIDSet.split("#",-1)
          var isTrue: Boolean = true
          if(prodcutArray.contains("10011") || prodcutArray.contains("10012")) {
            isTrue = Tools.checkIsRight(conn,phone)
          }
          log.info("------------�ж�" + phone + "�û������Ƿ񶩹���Ʒ10040��10041��" + isTrue)
          if(location && isPlanLUDay && isTrue) { //���ڵ�ǰ����
            Tools.SaveSMS(smsMsg,conn) //������ⷽʽ
            hbaseUtils.insertSMS(phone,countryCode,nowTime)
            for(id <- prodcutArray) {
              hbaseUtils.updateRecordCount(currentDate + id.substring(2))
            }
            //��¼�û����͹���
            for(record <- productIDs.substring(0,productIDs.length - 1).split("#",-1)) {
              if(rr.contains(record)) {
                rr.get(record).get.setIsPush("1")
              }
            }
            log.info("-------------" + phone+"�û������Ƽ����ųɹ�")
          } else {
            log.warn("�û����뿪��ǰ���һ���ڸ����µĻ�ӭ���ż�¼")
          }
        } else {
          log.warn("���ɶ���ʧ�ܣ�Ӫ����������ģ��Ϊ�ջ��Ʒ����ģ��Ϊ��")
        }

      }

    }
    hbaseUtils.delete(rowkey) //������ɣ���ɾ��������¼
    //д�û�ʵʱ��ǩ���û�λ�ñ����
    hbaseUtils.writeUserRealTimeLabel(userRealTimeLabel)
    //д�û����ͼ�¼��
    hbaseUtils.writeUserRecommendRecord(rr)

  }

   /**
     * �жϣ���ǩ-��Ʒ���Ƿ����û�����
     * @param jsonRule ��ǩ����
     * @param products �û��Ѷ�����Ч��Ʒ
     * @param pc ��Ʒ���ǹ���
     * @param pis ��Ʒ��Ϣ
     * @return
     */
  def isPush(jsonRule: String,products: List[String],countryCode: String, pc: ConcurrentHashMap[Integer, util.ArrayList[String]],
             pis: ConcurrentHashMap[Integer, ProductInfo],time: String): Boolean = {

    /**
      * ���ݲ�Ʒ�󶨵ı�ǩ�����ж��Ƿ��Ƽ�
      */
    var result: Boolean = isTrue(transRule(jsonRule,products,countryCode,pc,pis,time))
    result

  }

  /**
    * ����Json��ʽ����
    * @param jsonRule
    * @param products
    * @return
    */
  def transRule(jsonRule: String,products: List[String],countryCode: String,pc: ConcurrentHashMap[Integer, util.ArrayList[String]],
                pis: ConcurrentHashMap[Integer, ProductInfo],time: String): String = {

    try {
      val objectMapper = new ObjectMapper
      objectMapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true)
      val labelRule = objectMapper.readValue(jsonRule, classOf[LabelRule]) //����LabelRule����
      if(!products.isEmpty) {
        return parse(labelRule,products,countryCode,pc,pis,time)
      } else {
        return parse(labelRule,products,countryCode,pc,pis,time)
      }
//      parse(labelRule,products,countryCode,pc,pis,time)
    } catch {
      case e: Exception =>
        log.error("Json�ļ���������" + e)
        return "false"
    }

  }

   /**
     * �ݹ���������ж��Ƿ��Ƽ�
     * @param labelRule
     * @param products
     * @return
     */
  def parse(labelRule: LabelRule, products: List[String],countryCode: String,pc: ConcurrentHashMap[Integer, util.ArrayList[String]],
            pis: ConcurrentHashMap[Integer, ProductInfo],time: String): String = {

    var str = ""
    try {
      if (labelRule.isSingleRule) { //��������
        val rule = labelRule.getRule
        val value = labelRule.getValue
        val instructions = labelRule.getInstructions
        rule.trim match {
          case "=" => {
            if(products.contains(value)) { //�û��Ѷ�����ĳ��Ʒ�����Ƽ�
              str = str + true
            } else {
              str = str + false
            }
          }
          case "!=" => {
            if(products.contains(value)) { //�û�����ĳ��Ʒ
              /**
                * ��Ҫ����ǿ�������ϵ
                */
              if("2".equals(instructions)) {
                  val productKey = Tools.getProductKey(pis,labelRule.getValue,time)
                  log.debug("�����Ʒ��" + labelRule.getValue + "��ȡ��productKey:" + productKey)
                  if(pc.containsKey(productKey) && pc.get(productKey).contains(countryCode)) { //�ж��û��Ѷ�����Ʒ�Ƿ񸲸ǵ�ǰ���ι�
                    log.info("��Ʒ" + productKey + "��������" + countryCode + "�����Ƽ��ò�Ʒ")
                    str = str + false
                  } else {
                    log.info("��Ʒ" + productKey + "����������" + countryCode + ",�����Ƽ��ò�Ʒ")
                    str = str + true
                  }
              } else {
                str = str + false
              }
            } else { //�û�û����ĳ��Ʒ�����Ƽ�
              str = str + true
            }
          }
          case _ => {
            log.error("ϵͳ��ǰ��֧�ָ��߼��жϷ���" + rule)
          }
        }

      } else { //��Ϲ���
        if (null != labelRule.getRuleList && 0 < labelRule.getRuleList.size) {
          str = str + "("
          for(i <- 1 to labelRule.getRuleList.size()) {
            str = str + parse(labelRule.getRuleList.get(i-1),products,countryCode,pc,pis,time) //�ݹ����
            if(i == labelRule.getRuleList.size()) {
              str = str + ")"
            } else {
              if(null != labelRule.getLogic && labelRule.getLogic.toLowerCase.equals("and")) {
                str = str + " && "
              } else if(null != labelRule.getLogic && labelRule.getLogic.toLowerCase().equals("or")) {
                str = str + " || "
              }
            }
          }

        }
      }
    } catch {
      case ex : Exception => {
        log.error("�ݹ����JSON����"+ex)
      }
    }
    str
  }

  /**
    * �ַ����������߼����ʽ���ж�ֵ
    * @param str
    * @return
    */
  def isTrue(str: String): Boolean = {

    val manager:ScriptEngineManager = new ScriptEngineManager()
    val engine = manager.getEngineByName("js")
    val result: Object = engine.eval(str)
    log.info("JSON�ַ����߼��жϽ��Ϊ��" + str)
    result.asInstanceOf[Boolean]

  }


   /**
     * ��������ģ��
     * @param smsContend
     * @return
     */
   def parseHeadForChi(smsContend: String,countryName: String) = {

     val strs = smsContend.split("�Y���Z��",-1)
     val (greeting,ending) = (strs(0).replaceAll("�����Z","").trim.replaceAll("�����ҡ�",countryName),strs(1).trim)
     (greeting,ending)
   }

   /**
     * ����Ӣ��ģ��
     * @param smsContent
     * @return
     */
   def parseHeadForEng(smsContent: String,countryName: String) = {

     val strs = smsContent.split("Ending:",-1)
     val (greeting,ending) = (strs(0).replaceAll("Greeting","").trim.replace("��country��",countryName),strs(1).trim)
     (greeting,ending)
   }

   def getSmsContentChi(smsContent: String,product: String,productPrice: String,productSMSCD: String,contry: String): String = {
     val result = smsContent.replaceAll("���r��",productPrice).replaceAll("���aƷ���Q��",product).replaceAll("�����ž��a��",productSMSCD).replaceAll("�����ҡ�",contry)
     result
   }

   /**
     * ��Ʒ���ơ��۸񡢶��롢����
     * @param smsContent
     * @param product
     * @param productPrice
     * @param productSMSCD
     * @param country
     * @return
     */
   def getSmsContentEng(smsContent: String,product: String,productPrice: String,productSMSCD: String,country: String): String = {
     val result = smsContent.replaceAll("��price��",productPrice).replaceAll("��product name��",product).replaceAll("��SMS code��",productSMSCD).replaceAll("��country��",country)
     result
   }

   /**
     * ���ص�ǰϵͳʱ�估LU֮ǰʱ��
     * @param i
     */
   def getLuTime(i: Int) = {

     val nowTime = System.currentTimeMillis()
     val luTime = nowTime - i*60*60*1000*24
     (luTime.toString,nowTime.toString)
   }

   def SaveSMSList(conn: Connection,smsList: ArrayBuffer[SmsProfile]): Unit = {

     val sql = "insert into MNG_SMS_IMSI_INFO(PLAN_ID,CUSTOMER_ID,MSISDN,COUNTRY_CD,SEND_NUM," + "CRE_TIME,PRODUCT_ID_SET,PRODUCT_NUM,FLAG,SMS_CONTENT,SEND_LANGUAGE) values(?,?,?,?,?,?,?,?,?,?,?)"
     var ps: PreparedStatement = null
     try {
       conn.setAutoCommit(false)
       ps = conn.prepareStatement(sql)
       for(smsProfile <- smsList) {
         ps.setInt(1,smsProfile.getPlanID()) //Ӫ������ID
         ps.setString(2,smsProfile.getCustomerID()) //�ͻ�ID
         ps.setString(3,smsProfile.getMsisdn()) //�ֻ�����
         ps.setString(4,smsProfile.getCountryCode()) //���Ҵ���
         ps.setInt(5,1) //�ڼ��η��ͼ������
         ps.setTimestamp(6,new java.sql.Timestamp(System.currentTimeMillis())) //��¼����ʱ��
         ps.setString(7,smsProfile.getProductIDSet()) //�Ƽ���ƷID����
         ps.setInt(8,smsProfile.getProductNum()) //�Ƽ���Ʒ����
         ps.setString(9,smsProfile.getFlag()) //������־��Ĭ��Ϊ1
         ps.setString(10,smsProfile.getSmsContent()) //��������
         ps.setString(11,smsProfile.getSendLanguage()) //��������
         ps.addBatch()
       }
       ps.executeBatch()
       conn.commit()
     } catch {
       case ex: Exception => {
         log.error("��ǰ���ζ������ʧ�ܣ�"+ex)
       }
     } finally {
       if(ps != null) {
         ps.close()
       }
       if(conn != null) {
         conn.close()
       }
     }

   }

 }
