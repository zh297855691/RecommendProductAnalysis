import java.sql.ResultSet
import java.text.SimpleDateFormat
import java.util
import java.util.concurrent.ConcurrentHashMap
import java.util.{Calendar, Date}

import com.cmit.cmhk.GreetingMessageAnalysis.dateFormat3
import com.cmit.cmhk.entity.{MarketPlan, SmsProfile}
import com.cmit.cmhk.hbase.{HBaseUtils, OrderingInfo}
import com.cmit.cmhk.mysql.MySQLManager
import com.cmit.cmhk.utils.{LoadFile, LoadMySQLData, Tools, Utils}
import org.apache.hadoop.hbase.{CellUtil, TableName}
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.mutable
import org.apache.hadoop.hbase.filter.Filter
import org.apache.log4j.PropertyConfigurator

object test {
  PropertyConfigurator.configure(LoadFile.loadConfig("./cmhk-log.properties"))
  def main(args: Array[String]): Unit = {

    var str = "1#"
    println(str.split("#",-1)(1))
    if(str.split("#",-1)(1).equals("")) {
      println(true)
    }
    val list = List[String]("1","2")
    println(list.length)
//    var json = "{'singleRule':false,'logic':'AND','ruleList':[\n{'singleRule':true,'rule':'!=','attribute':'productId','value':'10001','instructions':'1'},\n{'singleRule':true,'rule':'!=','attribute':'productId','value':'10002','instructions':'1'},\n{'singleRule':true,'rule':'!=','attribute':'productId','value':'01016','instructions':'1'},\n{'singleRule':true,'rule':'!=','attribute':'productId','value':'01017','instructions':'1'},\n{'singleRule':true,'rule':'!=','attribute':'productId','value':'01021','instructions':'1'},\n{'singleRule':true,'rule':'!=','attribute':'productId','value':'01139','instructions':'1'},\n{'singleRule':true,'rule':'!=','attribute':'productId','value':'01140','instructions':'1'},\n{'singleRule':true,'rule':'!=','attribute':'productId','value':'01141','instructions':'1'},\n{'singleRule':true,'rule':'!=','attribute':'productId','value':'01142','instructions':'1'},\n{'singleRule':true,'rule':'!=','attribute':'productId','value':'01143','instructions':'1'},\n{'singleRule':true,'rule':'!=','attribute':'productId','value':'01144','instructions':'1'},\n{'singleRule':true,'rule':'!=','attribute':'productId','value':'01145','instructions':'1'},\n{'singleRule':true,'rule':'!=','attribute':'productId','value':'01146','instructions':'1'},\n{'singleRule':true,'rule':'!=','attribute':'productId','value':'01147','instructions':'1'},\n{'singleRule':true,'rule':'!=','attribute':'productId','value':'01148','instructions':'1'},\n{'singleRule':true,'rule':'!=','attribute':'productId','value':'01149','instructions':'1'},\n{'singleRule':true,'rule':'!=','attribute':'productId','value':'01150','instructions':'1'}]}"
//    val strchn = "中國移動香港: 貴為特選客戶，您可於【國家】以優惠價低至[HK$【價格】 （【包內天數】日）]申請數據漫遊組合，享用無憂數據漫遊服務！立即回覆此短訊："
//    val streng = "CMHK: As our privilege customer, you can enjoy data roaming service in 【country】 with discounted price as low as [HK$【price】 （【days】 days）] for Data roaming pass. Reply this SMS to subscribe!\nEnding:Reply SMS will apply Roaming SMS charge. Terms and conditions apply. EN29458888／UN62264926"
//    println(str.replace("HK$【價格】 (【包內天數】日)","HK$4(3日)"))
//    val s = "HK$4(3日)/"
//    println(streng.substring(0,streng.indexOf("[")))
//    println(streng.substring(streng.indexOf("]") + 1))
//    println(s.substring(0,s.length - 1))
//    println(str.substring(0,26) + s.substring(0,s.length -1) + str.substring(43))

//    val list = List("00036","00037")
//    list.map(_.substring(2)).foreach(println _)
//    val conn = MySQLManager.getMySQLManager(true).getConnection.get
//    val value = (Tools.isOVL(conn),Tools.getCountryCode(conn),Tools.getRoamingCountryCode(conn))
//    System.setProperty("java.security.auth.login.config", "C:\\Users\\lenovo\\Desktop\\jaas-cache.conf")
//    System.setProperty("java.security.krb5.conf", "C:\\Users\\lenovo\\Desktop\\krb5.conf")
//    val ugi = HBaseUtils.initUGI("mrs@ZUHU1.COM","C:\\Users\\lenovo\\Desktop\\mrs.keytab")
//    val hbaseUtils = new HBaseUtils("master02-kf.zuhu1.com:2181,master03-kf.zuhu1.com:2181,master01-kf.zuhu1.com:2181",ugi)
//    val countryCode = Utils.getCountryCodeByADDR("85292040018",value._2,value._3,hbaseUtils)
//    println(countryCode)

//    println(dateFormat3.format(System.currentTimeMillis()).toInt)
//    val dateFormat1 = new SimpleDateFormat("HHmmss")
//    println(dateFormat1.format(System.currentTimeMillis()))
//    val s = new StringBuilder
//    val str = "43,"
//    s.append(str)
//    println(s.substring(0,str.length - 1).split(",",-1).foreach(println _))
    /**
      * HBase Test
      */
//    System.setProperty("java.security.auth.login.config", "C:\\Users\\lenovo\\Desktop\\mrs\\jaas-cache.conf")
//    System.setProperty("java.security.krb5.conf", "C:\\Users\\lenovo\\Desktop\\mrs\\cs\\krb5.conf")
//    val ugi = HBaseUtils.initUGI("mrs@ZUHU2.COM","C:\\Users\\lenovo\\Desktop\\mrs\\cs\\mrs.keytab")
//    val hBaseUtils = new HBaseUtils("master03-cs.zuhu2.com:2181,master02-cs.zuhu2.com:2181,master01-cs.zuhu2.com:2181",ugi)
////    println(hBaseUtils.isInThisCountry("","",""))
//    println(hBaseUtils.isNewestSMS("85290100010","20180703151033"))
//    hBaseUtils.productCountInsert("2018062767154448",3L)
//    val hBaseUtils = new HBaseUtils("master02-kf.zuhu1.com:2181,master03-kf.zuhu1.com:2181,master01-kf.zuhu1.com:2181",ugi)
//    println(hBaseUtils.connection)
//    println("*****************************"+hBaseUtils.isNewestSMS("85290100035","20180606115031"))



//    for(s <- hBaseUtils.getHBaseList()) {
//      println(s)
//    }
//    val str = new ArrayBuffer[String]()
//    str += "20180426093000" + "85252238131" + "$" + "1" + "#" + "wsm_om" + "#" + "chn"
//    str += "20180425233000" + "85252238132" + "$" + "1" + "#" + "wsm_om" + "#" + "eng"
//    str += "20180426083000" + "85252238133" + "$" + "1" + "#" + "wsm_om" + "#" + "tha"
//    println(Tools.isPostpay("85252238131",hBaseUtils))
//    println(Tools.isPostpay("85252238133",hBaseUtils))
//    println(Tools.isPostpay("85252238135",hBaseUtils))
//    hBaseUtils.delete("A2018042609300085252238131")
//    hBaseUtils.getHBaseList().foreach(println _)
//    hBaseUtils.insertGreetingMessageTime(str,"mrs:RPA_MRS_SMS_TIME")
//    println(hBaseUtils.findOrderingInfo("1001_1_20170101"))
//    println(hBaseUtils.getRecordByRowkey("8523843"))
    /**
      * MySQL
      */
//    val conn = MySQLManager.getMySQLManager(true).getConnection.get
//    println(Tools.checkIsPlanDay(conn,"85238901231",13))
//    val sql = "select count(SEND_NUM) as send from MNG_SMS_IMSI_INFO"
//    try {
//      val st = conn.createStatement
//      val rs = st.executeQuery(sql)
//      while (rs.next()) {
//        println(rs.getLong("send"))
//      }
//    }

//    val smsMsg = new SmsProfile
//    smsMsg.setMsisdn("8613828749815")
//    smsMsg.setCountryCode("CHN")
//    smsMsg.setCustomerID("CMCC")
//    smsMsg.setPlanID(128)
//    smsMsg.setSmsContent("欢迎来到中国" + ";" + "订购亚洲区Go Pass包优惠进行中") //短信内容
//    smsMsg.setFlag("1")//漫游欢迎短信触发
//    smsMsg.setProductIDSet("12,23,4")
//    smsMsg.setProductNum(3)
//    Tools.SaveSMS(smsMsg,conn)
//    println("插入成功")

//    val record = "A2018071008095185290100001 "
//    val loadMySQLData = LoadMySQLData.getLoadMySQLData()
//    loadMySQLData.initValue("HKGPP",conn)
//    val map = Tools.isOVL(conn)
//    Utils.processMarketingSMS(record,hBaseUtils,"HKGPP",loadMySQLData,map,"1#1#THA",conn)

//    val customerID = "HKGPP"
//    val  result: ArrayBuffer[MarketPlan] = LoadMySQLData.getPlans(customerID,conn)
//    for(r: MarketPlan <- result) {
//      println(r.getCustomerID+" : "+r.getEffectTM+" : "+r.getExpireTM+" : "+r.getPlanID+" : "+r.getPlanStatus)
//    }

//    val str = "{'singleRule':false,'logic':'and','ruleList':[{'singleRule':true,'rule':'!=','attribute':'1','value':'8'},{'singleRule':true,'rule':'=','attribute':'2','value':'8'},{'singleRule':false,'logic':'or','ruleList':[{'singleRule':true,'rule':'=','attribute':'3','value':'8'},{'singleRule':true,'rule':'=','attribute':'4','value':'8'}]}]}"
//    val value = jsonTest.transRule(str)
//    val list = List("4","5","8")
//    for(i <- 0 to list.size) {
//      println(list(i))
//    }
//    println(Utils.isPush(str,list))

//    println(list.contains("4"))
//    if(value) {
//      println()
//    }

//    val str = "133#23#"
//    println(str.substring(0,str.length - 1))
//    val dateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
//    val dateFormat2 = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
//    val str = "2018/01/29 20:48:35"
//    val date = dateFormat2.parse(str)
//    println(dateFormat.format(date))
//
//    val calendar = Calendar.getInstance()
//    calendar.set(Calendar.HOUR_OF_DAY,calendar.get(Calendar.HOUR_OF_DAY) - 12)
//    println(dateFormat.format(calendar.getTime))
//    println("20180129204835A33847294".substring(0,14)+"#" + "20180129204835A33847294".substring(14))
//    println("20180312".compareTo("20180322"))
//    println("Asde".equals("asde"))
//
//    val newd = new OrderingInfo
//    println(newd.getActivationDate)
//    val scan = new Scan()
//    val t = HBasetest.getConnection.getTable(TableName.valueOf("rctl2.TRADE_INFO_DETAIL"))
//    val sc = t.getScanner(scan)
//    val it: util.Iterator[Result] = sc.iterator
//    while (it.hasNext) {
//      val s = it.next()
//      val cells = s.rawCells()
//      println(Bytes.toString(s.getRow))
//      for(cell <- cells) {
//        val k = Bytes.toString(CellUtil.cloneRow(cell))
//        val family = Bytes.toString(CellUtil.cloneFamily(cell))
//        val q = Bytes.toString(CellUtil.cloneQualifier(cell))
//        val v = Bytes.toString(CellUtil.cloneValue(cell))
//        System.out.println("ROWKEY为：" + k + "Family:" + family + " 列名：" + q + " 值为：" + v)
//      }
//    }
//    val str = "2018-04-27 10:04:38.0"
//    println(Utils.getFormatTime2(str))

  }

}
