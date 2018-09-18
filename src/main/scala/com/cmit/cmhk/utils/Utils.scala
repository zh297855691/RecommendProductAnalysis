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
    * 返回当前时刻前多少个小时
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
    * 格式化时间格式为yyyyMMddHHmmss
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
    * 格式化时间格式为yyyyMMddHHmmss
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
    * 根据关口号获取国家代码
    * @param str
    * @return
    */
  def getCountryCodeByADDR(str: String,countryCodeMap: ConcurrentHashMap[String,String],myCountryCodeMap: ConcurrentHashMap[String,String],hBaseUtils: HBaseUtils): Option[String] = {

    var countryCode = hBaseUtils.getCountryCodeByADDR(str)
    if(countryCode.isEmpty) { //如果表内无关口号对应漫游运营商国代码，则需要解析
      val result = extractCountryCode(str,countryCodeMap)
      if(result.isDefined) {
        if(myCountryCodeMap.containsKey(result.get)) {
          countryCode = Some(myCountryCodeMap.get(result.get))
          //存入HBase表：mrs:RPA_MRS_MSC_COUNTRY_CODE(关口号与运营商国家代码表)
          hBaseUtils.saveCountryCode(str,countryCode.get)
        }
      }
    }
    countryCode
  }

  /**
    * 根据规则关口号匹配漫游国家代码
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
     * 处理逻辑入口函数
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
    log.debug("进行营销推荐逻辑处理，该记录rowkey：" + rowkey + ",value:" + record)
    val dateFormat2 = new SimpleDateFormat("yyyyMMdd")
    val currentDate = dateFormat2.format(System.currentTimeMillis())
    val countryCode = record.split("#", -1)(2)
    //如果为日本用户，直接不进行推荐
    if(countryCode == "JPN"){
        return
    }

    val userRealTimeLabel = new UserRealTimeLabel()
    //获取该用户手机号码：香港用户手机号码11位（含国际码85）
    val phone = rowkey.trim.substring(15)
    //漫游短信发送时间
    val time = rowkey.substring(1,15)
    //判断用户是否为白名单用户
    val ovl = ovlMap.containsKey(phone.substring(3)+"1") || ovlMap.containsKey(phone.substring(3)+"3")
    //判断90天内是否有订购产品记录
    val orderRecord = ovlMap.containsKey(phone.substring(3)+"2")
    /**
      * 99999一般客户预付费
      * 99998企业客户预付费
      */
    val postpay = Tools.isPostpay(phone,hbaseUtils)
    //获取该用户的选择的语言
    val language = Tools.getLanguage(phone.substring(3),hbaseUtils)
    //返回当前用户订购产品的信息
    val productInfo = hbaseUtils.findOrderingInfo(phone)
    log.info("当前用户订购有效的产品信息：" + productInfo)
    //存放可能需要推荐产品Key
    val recommendProducts = new ArrayBuffer[(Int,Int,String,String,Int)]()
    val NewrecommendProducts = new ArrayBuffer[(Int,Int,String,String,Int)]()
    var promap = Map[String,Int]()
    //非白名单且后付费且存在选择语言的用户进入营销分析
    log.debug("当前用户" + phone+"是否为白名单用户：" + ovl + ",预付费用户类型：" + postpay + ",用户选择的语言：" + language + ",用户当前位置:" + countryCode)
    //用户实时标签表&用户位置更新表
    userRealTimeLabel.setMsisdn(phone)
    userRealTimeLabel.setCountryCode(countryCode)
    userRealTimeLabel.setLuTime(time)
    userRealTimeLabel.setSource("WSMS")
    userRealTimeLabel.setIsPayList(postpay)
    userRealTimeLabel.setOrderProductID(productInfo.get._2)

    var rr: Map[String,UserRecommendRecord] = Map()

    if(!ovl && "99999".equals(postpay) && language != 0) {
      userRealTimeLabel.setIsWhiteList("0")
      log.debug("当前用户实时标签：" + userRealTimeLabel)
      val mps: ArrayBuffer[MarketPlan] = loadMySQLData.marketPlan //营销方案
      val pis: ConcurrentHashMap[Integer, ProductInfo] = loadMySQLData.productInfo //营销产品
      val uls: ConcurrentHashMap[Integer, UserLabel] = loadMySQLData.userLabel //用户标签
      val pps: ArrayBuffer[PlanProduct] = loadMySQLData.planProduct //方案推荐产品绑定关系
      val psi: ConcurrentHashMap[String,String] = loadMySQLData.planSMSInfo//营销方案短信模板
      val pc: ConcurrentHashMap[Integer, util.ArrayList[String]] = loadMySQLData.productCountry //产品覆盖国家
      val ppi: ConcurrentHashMap[String,String] = loadMySQLData.productSMSInfo//营销产品短信模板
      val cn: ConcurrentHashMap[String,String] = loadMySQLData.countryName //国家中英文名称
      val dateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
      val currentTime = dateFormat.format(System.currentTimeMillis())
      /**
        * 特殊产品需求，根据用户90天套餐情况分群
        */
      val productSet = List[String]("00040","00041","10040","10041","00011","00012","10011","10012")

      //记录可推送产品集(90天过滤规则)
      var speacialSet = List[String]()
      var isDelete = 0
      //该营销方案需应满足有效期内且已启用
      for( mp: MarketPlan <- mps if getFormatTime2(mp.getEffectTM).compareTo(currentTime) <= 0
        && getFormatTime2(mp.getExpireTM).compareTo(currentTime) >= 0 && mp.getPlanStatus.equals("0")) {
        /**
          * 根据用户漫游欢迎短信时间：
          * 1、判断该方案绑定标签的生效期是否涵盖用户漫游欢迎短信的时间
          * 2、判断该方案绑定营销产品的生效期是否涵盖用户漫游欢迎短信的时间
          */
        for(pp: PlanProduct <- pps if pp.getPlanID == mp.getPlanID) {

          /**
            * 获取ProductID绑定有效期内的ProductKey
            * 获取LabelID绑定有效期内的LabelKey
            */
          val productKey = Tools.getProductKey(pis,pp.getProductID,time)
          log.debug("根据当前方案" + mp.getPlanID + "的产品ID" + pp.getProductID + "和lu时间" + time + "获取到productKey:" + productKey)
          val labelKey = Tools.getLabelKey(uls,pp.getLabelID,time)
          log.debug("根据当前方案" + mp.getPlanID + "的产品ID" + pp.getLabelID + "和lu时间" + time + "获取到labelKey:" + labelKey)

          //00为激活类产品：产品名称，价格，天数，10为预购类产品：短信编码
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
//          log.debug("当前用户" + phone + "订购产品为：" + products)
          //判断理论上可以推送的产品集合，默认不推
          log.debug("判断标签" + labelKey + "是否属于该用户标签集")
          if(uls.containsKey(labelKey)) {
            log.debug("用户标签集包含" + labelKey + "标签，进行标签规则推荐判断")
            flag = isPush(uls.get(labelKey).getConditionResult,products,countryCode,pc,pis,time)
            log.debug(phone + "用户标签逻辑判断推送结果：" + flag)
          }else{
            log.debug("用户标签集不包含" + labelKey + "标签")
          }
          //获取当天日期
          var limit = 0l
          var isLimit: Boolean = false
//          log.info("产品ID：" + pp.getProductID + "当天限制量为："+pp.getSendLimit)
          if("".equals(pp.getSendLimit)) {
            isLimit = true
            log.info("产品ID：" + pp.getProductID + "当天无限制量，可推荐")
          } else if("0".equals(pp.getSendLimit)) {
            isLimit = false
            log.info("产品ID：" + pp.getProductID + "当天限制量为0，不可推荐")
          } else {
            limit = pp.getSendLimit.toLong
            isLimit = hbaseUtils.productCountInsert(currentDate + pp.getProductID.substring(2),limit)
          }

          if(flag) {

            //记录用户推送过程
            val urr = new UserRecommendRecord
            urr.setPlanID(pp.getPlanID.toString)
            urr.setMsisdn(phone)
            urr.setCountryCode(countryCode)
            urr.setLuTime(time)
            urr.setPri(pp.getProductPriority)
            urr.setProductID(pp.getProductID)
            rr += (pp.getProductID -> urr)

            //该当前产品覆盖用户所在国家时才考虑推荐
            if(pc.containsKey(productKey) && pc.get(productKey).contains(countryCode)) { //剔除
              log.info(phone+"产品ID：" + pp.getProductID + "可以推荐。")
              if(isLimit) {
                speacialSet = speacialSet :+ pp.getProductID
                recommendProducts += ((productKey,productKey00,pp.getProductID,pis.get(productKey).getProductType,pp.getProductPriority.toInt))
              } else {
                log.info("产品："+ pp.getProductID +",超过当日允许最大限制！")
              }
            } else {
              log.info("产品：" + pp.getProductID + "未覆盖当前国家【" + countryCode +"】")
            }
          }
        }
        log.debug("当前用户" + phone + "推荐产品集：" + recommendProducts)
        /**
          * 剔除不符合逻辑的产品
          */
        if((speacialSet.contains("10040") || speacialSet.contains("10041")) && (speacialSet.contains("10011") || speacialSet.contains("10012"))) {
          isDelete = 1 //表示同时可推送10040，10041和10011，10012
        } else if ((!speacialSet.contains("10040") && !speacialSet.contains("10041")) && (speacialSet.contains("10011") || speacialSet.contains("10012"))) {
          isDelete = 2 //表示可推10011,10012
        } else if ((speacialSet.contains("10040") || speacialSet.contains("10041")) && (!speacialSet.contains("10011")  && !speacialSet.contains("10012"))) {
          isDelete = 3 //表示可推10041,10040
        }

        /**
          * 关于90天的过滤逻辑
          */
        log.debug("-------------进行90天过滤-------------")
        for(record <- recommendProducts) {
          isDelete match {
            case 1 => {
              if(orderRecord) { //存在90天订购记录，不推10040和10041
                record._3 match {
                  case "10011" => NewrecommendProducts += record
                  case "10012" => NewrecommendProducts += record
                  case _ => log.info("当前用户：" + phone +"存在90天订购记录，则不推送：" + record._3)
                }
              } else { //不存90天订购记录
                record._3 match {
                  case "10040" => NewrecommendProducts += record
                  case "10041" => NewrecommendProducts += record
                  case _ => log.info("当前用户：" + phone +"不存在90天订购记录，则不推送：" + record._3)
                }
              }
            }
            case 3 => {
              if(!orderRecord) { //不存在90天订购记录
                NewrecommendProducts += record
              } else {
                log.info("当前用户：" + phone +"存在90天订购记录，则不推送：" + record._3)
              }
            }
            case _ => NewrecommendProducts += record
          }
        }
        log.debug("当前用户过滤后的推荐产品集为：" + NewrecommendProducts)
        /**
          * 产品优先级过滤
          */
//        log.debug("------------进行产品优先级判断-----------------")
        for((productKey,productKey00,productID,productType,priority) <- NewrecommendProducts) {
          //保存每种产品类型的最小优先级(1为最高优先级)
          if(promap.contains(productType) && promap.get(productType).get > priority) {
            promap += (productType -> priority)
          } else {
            promap += (productType -> priority)
          }
        }

        /**
          * 营销短信生成代码区
          * 营销方案的问候短信模板内容(逻辑为一个营销方案生成一个营销短信)
          */
        var smsContentHead = ("","")
        var productSmsInfo = ""
        var productSmsInfoList = List[(Int,String)]()
        if(language == 1 && psi.containsKey(mp.getPlanID + "02") && cn.containsKey(countryCode+"02")) { //中文版本
          smsContentHead = parseHeadForChi(psi.get(mp.getPlanID + "02"),cn.get(countryCode+"02"))
        } else if (language == 2 && psi.containsKey(mp.getPlanID + "01") && cn.containsKey(countryCode + "01")) { //英文版本
          smsContentHead = parseHeadForEng(psi.get(mp.getPlanID + "01"),cn.get(countryCode + "01"))
        } else {
          log.warn("【注意】当前营销方案未配置中文或英文营销短信模板，营销方案：" + mp.getPlanID)
        }

        var smsContentEndList = List[(Int,String)]()
        var productIDs = new StringBuilder
        //根据优先级最后决定需要推荐的产品，并判断该产品是否涵盖当前运营商国
        for((productKey,productKey00,productID,productType,priority) <- NewrecommendProducts if (promap.contains(productType) && priority == promap.get(productType).get)) {
          productIDs.append(productID + "#")
          language match {
            case 1 => {
              if(ppi.containsKey(productKey + "02") && pis.containsKey(productKey) && pis.containsKey(productKey00) && cn.containsKey(countryCode + "02")) {
                productSmsInfoList = productSmsInfoList :+ (pis.get(productKey00).getProductPrice.toInt -> pis.get(productKey00).getProductDays)
                smsContentEndList = smsContentEndList :+ (pis.get(productKey00).getProductPrice.toInt -> getSmsContentChi(ppi.get(productKey + "02"),
                  pis.get(productKey00).getProductName,pis.get(productKey00).getProductPrice,pis.get(productKey).getProductSMSCD,cn.get(countryCode + "02")))
              } else {
                log.warn("【注意】产品ID：" + productID + ",产品ProductKey:" + productKey + " 未配置中文短信模板！")
              }
            }
            case 2 => {
              if(ppi.containsKey(productKey + "01") && pis.containsKey(productKey) && pis.containsKey(productKey00) && cn.containsKey(countryCode + "01")) {
                productSmsInfoList = productSmsInfoList :+ (pis.get(productKey00).getProductPrice.toInt -> pis.get(productKey00).getProductDays)
                smsContentEndList = smsContentEndList :+ (pis.get(productKey00).getProductPrice.toInt -> getSmsContentEng(ppi.get(productKey + "01"),
                  pis.get(productKey00).getProductNameEN,pis.get(productKey00).getProductPrice,pis.get(productKey).getProductSMSCD,cn.get(countryCode + "01")))
              } else {
                log.warn("【注意】产品ID：" + productID + ",产品ProductKey:" + productKey +":" +productKey00 + " 未配置英文短信模板！")
              }
            }
          }
        }
        var smsContentHeadFront = ""
        for(list <- productSmsInfoList.sorted) {
          language match {
            case 1 => {//中文
              productSmsInfo = productSmsInfo + "HK$" + list._1 +"（" + list._2 + "日）" + "/"
            }
            case 2 => {//英文
              if(list._2.toInt > 1) {
                productSmsInfo = productSmsInfo + "HK$" + list._1 +"(" + list._2 + "days)" + "/"
              } else {
                productSmsInfo = productSmsInfo + "HK$" + list._1 +"(" + list._2 + "day)" + "/"
              }
            }
          }
        }
        var smsContentEnd = ""
        var sort = 1 //标记所要推荐产品序号
        if(smsContentEndList.length == 1) {
          for(list <- smsContentEndList.sorted) {
            smsContentEnd = list._2 + "\n"
          }
        } else if(smsContentEndList.length > 1) {
          for(list <- smsContentEndList.sorted) {
            language match {
              case 1 => { //中文
                smsContentEnd = smsContentEnd + sort + "." + list._2 + "\n"
              }
              case 2 => { //英文
                smsContentEnd = smsContentEnd + sort + ". " + list._2 + "\n"
              }
            }
            sort = sort + 1
          }
        }

        if(language == 1 && !"".equals(smsContentHead._1) && !"".equals(productSmsInfo)) { //中文
          smsContentHeadFront = smsContentHead._1.substring(1,smsContentHead._1.indexOf("[")) +
            productSmsInfo.substring(0,productSmsInfo.length - 1) + smsContentHead._1.substring(smsContentHead._1.indexOf("]") + 1)
        } else if (language == 2 && !"".equals(smsContentHead._1) && !"".equals(productSmsInfo)) { //英文
          smsContentHeadFront = smsContentHead._1.substring(1,smsContentHead._1.indexOf("[")) +
            productSmsInfo.substring(0,productSmsInfo.length - 1) + smsContentHead._1.substring(smsContentHead._1.indexOf("]") + 1)
        }

        /**
          * 短信发送区
          */
        val smsMsg = new SmsProfile
        if(!smsContentHead._1.equals("") && !smsContentHead._2.equals("") && !smsContentEnd.equals("")) {
          smsMsg.setSendLanguage(language.toString)
          smsMsg.setMsisdn(phone)
          smsMsg.setCountryCode(countryCode)
          smsMsg.setCustomerID(customerID)
          smsMsg.setPlanID(mp.getPlanID)
          smsMsg.setSmsContent(smsContentHeadFront + "\n" + "\n" + smsContentEnd + "\n" +smsContentHead._2) //短信内容
          smsMsg.setFlag("1")//漫游欢迎短信触发
          smsMsg.setProductIDSet(productIDs.substring(0,productIDs.length - 1))
          smsMsg.setProductNum(productIDs.substring(0,productIDs.length - 1).split("#",-1).length)

          /**
            * 根据当前漫游欢迎短信发送时间之后GGSN话单，返回（最新GGSN记录时间且大于当前短信生成时间的记录，国家）
            */
          val isLeave = hbaseUtils.isInThisCountry(countryCode,phone,time)
          log.debug("------------通过GGSN话单判断当前用户" + phone + "最新位置：" + isLeave)
          log.info(phone + " GGSN is Leave:" + isLeave)
          /**
            * 返回（判断是否为存在最新欢迎短信，最新短信记录时间，国家）
            */
          val isNewestSMS = hbaseUtils.isNewestSMS(phone,time)
          log.debug("-------------判断当前用户" + phone + "的LU时间" + time + "是否为最新欢迎短信：" + isNewestSMS._1)
          log.info(phone + " SMS is Newest:" + isNewestSMS)

          /**
            * 校验是否满足LU过滤天数
            */
//          val isPlanLUDay = Tools.checkIsPlanDay(conn,phone,mp.getPlanLUDay.toInt,countryCode)
          val (luTime,nowTime) = getLuTime(mp.getPlanLUDay.toInt)
          val isPlanLUDay: Boolean = hbaseUtils.checkIsPlanDay(phone,countryCode,luTime)
          log.debug("----------------校验是否满足LU过滤天数：" + isPlanLUDay)

          /**
            * 根据最新欢迎短信和GGSN话单共同判断用户的最新位置
            */

          var location: Boolean = false
          if("".equals(isLeave._1) || "".equals(isLeave._2)) { //无GGSN记录
            if(isNewestSMS._1) { //当前为最新短信记录，且无大于当前短信时间的GGSN记录
              location = true
            }
          } else { //存在最新GGSN
            if(isNewestSMS._1) { //当前为最新短信记录
              if(countryCode.equals(isLeave._2)) { //用户当前国家和GGSN话单国家相同
                location = true
              }
            } else if(isNewestSMS._2.compareTo(isLeave._1) < 0 && countryCode.equals(isLeave._2)) { //最新短信时间小于最新GGSN话单时间
              if(isNewestSMS._3.equals(countryCode)) { //如果最新短信国家与当前短信国家相同，则为false
                location = false
//                location = true
              } else {
                location = true
//                location = false
              }
            }
          }
          log.debug("-----------根据最新欢迎短信和GGSN话单共同判断当前用户" + phone + "是否已离开：" + !location)
          log.info(phone + " 用户位置信息："+location + "，15天LU过滤逻辑：" + isPlanLUDay)

          /**
            * 若当天用户已经推了10040或10041，则不推荐10011和10012
            */
          val prodcutArray = smsMsg.getProductIDSet.split("#",-1)
          var isTrue: Boolean = true
          if(prodcutArray.contains("10011") || prodcutArray.contains("10012")) {
            isTrue = Tools.checkIsRight(conn,phone)
          }
          log.info("------------判断" + phone + "用户当天是否订购产品10040或10041：" + isTrue)
          if(location && isPlanLUDay && isTrue) { //仍在当前国家
            Tools.SaveSMS(smsMsg,conn) //单条入库方式
            hbaseUtils.insertSMS(phone,countryCode,nowTime)
            for(id <- prodcutArray) {
              hbaseUtils.updateRecordCount(currentDate + id.substring(2))
            }
            //记录用户推送过程
            for(record <- productIDs.substring(0,productIDs.length - 1).split("#",-1)) {
              if(rr.contains(record)) {
                rr.get(record).get.setIsPush("1")
              }
            }
            log.info("-------------" + phone+"用户生成推荐短信成功")
          } else {
            log.warn("用户已离开当前国家或存在更加新的欢迎短信记录")
          }
        } else {
          log.warn("生成短信失败，营销方案短信模板为空或产品短信模板为空")
        }

      }

    }
    hbaseUtils.delete(rowkey) //处理完成，则删除该条记录
    //写用户实时标签表及用户位置变更表
    hbaseUtils.writeUserRealTimeLabel(userRealTimeLabel)
    //写用户推送记录表
    hbaseUtils.writeUserRecommendRecord(rr)

  }

   /**
     * 判断（标签-产品）是否向用户推送
     * @param jsonRule 标签规则
     * @param products 用户已订购生效产品
     * @param pc 产品覆盖国家
     * @param pis 产品信息
     * @return
     */
  def isPush(jsonRule: String,products: List[String],countryCode: String, pc: ConcurrentHashMap[Integer, util.ArrayList[String]],
             pis: ConcurrentHashMap[Integer, ProductInfo],time: String): Boolean = {

    /**
      * 根据产品绑定的标签规则判断是否推荐
      */
    var result: Boolean = isTrue(transRule(jsonRule,products,countryCode,pc,pis,time))
    result

  }

  /**
    * 解析Json格式规则
    * @param jsonRule
    * @param products
    * @return
    */
  def transRule(jsonRule: String,products: List[String],countryCode: String,pc: ConcurrentHashMap[Integer, util.ArrayList[String]],
                pis: ConcurrentHashMap[Integer, ProductInfo],time: String): String = {

    try {
      val objectMapper = new ObjectMapper
      objectMapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true)
      val labelRule = objectMapper.readValue(jsonRule, classOf[LabelRule]) //返回LabelRule对象
      if(!products.isEmpty) {
        return parse(labelRule,products,countryCode,pc,pis,time)
      } else {
        return parse(labelRule,products,countryCode,pc,pis,time)
      }
//      parse(labelRule,products,countryCode,pc,pis,time)
    } catch {
      case e: Exception =>
        log.error("Json文件解析错误：" + e)
        return "false"
    }

  }

   /**
     * 递归解析规则及判断是否推荐
     * @param labelRule
     * @param products
     * @return
     */
  def parse(labelRule: LabelRule, products: List[String],countryCode: String,pc: ConcurrentHashMap[Integer, util.ArrayList[String]],
            pis: ConcurrentHashMap[Integer, ProductInfo],time: String): String = {

    var str = ""
    try {
      if (labelRule.isSingleRule) { //单个条件
        val rule = labelRule.getRule
        val value = labelRule.getValue
        val instructions = labelRule.getInstructions
        rule.trim match {
          case "=" => {
            if(products.contains(value)) { //用户已订购了某产品，则推荐
              str = str + true
            } else {
              str = str + false
            }
          }
          case "!=" => {
            if(products.contains(value)) { //用户订购某产品
              /**
                * 需要检验强弱互斥关系
                */
              if("2".equals(instructions)) {
                  val productKey = Tools.getProductKey(pis,labelRule.getValue,time)
                  log.debug("规则产品：" + labelRule.getValue + "获取的productKey:" + productKey)
                  if(pc.containsKey(productKey) && pc.get(productKey).contains(countryCode)) { //判断用户已订购产品是否覆盖当前漫游国
                    log.info("产品" + productKey + "包含国家" + countryCode + "，不推荐该产品")
                    str = str + false
                  } else {
                    log.info("产品" + productKey + "不包含国家" + countryCode + ",可以推荐该产品")
                    str = str + true
                  }
              } else {
                str = str + false
              }
            } else { //用户没订购某产品，则推荐
              str = str + true
            }
          }
          case _ => {
            log.error("系统当前不支持该逻辑判断符：" + rule)
          }
        }

      } else { //组合规则
        if (null != labelRule.getRuleList && 0 < labelRule.getRuleList.size) {
          str = str + "("
          for(i <- 1 to labelRule.getRuleList.size()) {
            str = str + parse(labelRule.getRuleList.get(i-1),products,countryCode,pc,pis,time) //递归解析
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
        log.error("递归解析JSON错误："+ex)
      }
    }
    str
  }

  /**
    * 字符串解析成逻辑表达式并判断值
    * @param str
    * @return
    */
  def isTrue(str: String): Boolean = {

    val manager:ScriptEngineManager = new ScriptEngineManager()
    val engine = manager.getEngineByName("js")
    val result: Object = engine.eval(str)
    log.info("JSON字符串逻辑判断结果为：" + str)
    result.asInstanceOf[Boolean]

  }


   /**
     * 解析中文模板
     * @param smsContend
     * @return
     */
   def parseHeadForChi(smsContend: String,countryName: String) = {

     val strs = smsContend.split("結束語：",-1)
     val (greeting,ending) = (strs(0).replaceAll("問候語","").trim.replaceAll("【國家】",countryName),strs(1).trim)
     (greeting,ending)
   }

   /**
     * 解析英文模板
     * @param smsContent
     * @return
     */
   def parseHeadForEng(smsContent: String,countryName: String) = {

     val strs = smsContent.split("Ending:",-1)
     val (greeting,ending) = (strs(0).replaceAll("Greeting","").trim.replace("【country】",countryName),strs(1).trim)
     (greeting,ending)
   }

   def getSmsContentChi(smsContent: String,product: String,productPrice: String,productSMSCD: String,contry: String): String = {
     val result = smsContent.replaceAll("【價格】",productPrice).replaceAll("【產品名稱】",product).replaceAll("【短信編碼】",productSMSCD).replaceAll("【國家】",contry)
     result
   }

   /**
     * 产品名称、价格、短码、国家
     * @param smsContent
     * @param product
     * @param productPrice
     * @param productSMSCD
     * @param country
     * @return
     */
   def getSmsContentEng(smsContent: String,product: String,productPrice: String,productSMSCD: String,country: String): String = {
     val result = smsContent.replaceAll("【price】",productPrice).replaceAll("【product name】",product).replaceAll("【SMS code】",productSMSCD).replaceAll("【country】",country)
     result
   }

   /**
     * 返回当前系统时间及LU之前时间
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
         ps.setInt(1,smsProfile.getPlanID()) //营销方案ID
         ps.setString(2,smsProfile.getCustomerID()) //客户ID
         ps.setString(3,smsProfile.getMsisdn()) //手机号码
         ps.setString(4,smsProfile.getCountryCode()) //国家代码
         ps.setInt(5,1) //第几次发送激活短信
         ps.setTimestamp(6,new java.sql.Timestamp(System.currentTimeMillis())) //记录创建时间
         ps.setString(7,smsProfile.getProductIDSet()) //推荐产品ID集合
         ps.setInt(8,smsProfile.getProductNum()) //推荐产品个数
         ps.setString(9,smsProfile.getFlag()) //触发标志，默认为1
         ps.setString(10,smsProfile.getSmsContent()) //短信内容
         ps.setString(11,smsProfile.getSendLanguage()) //短信语种
         ps.addBatch()
       }
       ps.executeBatch()
       conn.commit()
     } catch {
       case ex: Exception => {
         log.error("当前批次短信入库失败："+ex)
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
