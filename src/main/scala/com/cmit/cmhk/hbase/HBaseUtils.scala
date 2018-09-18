package com.cmit.cmhk.hbase

import java.security.PrivilegedExceptionAction
import java.sql.SQLException
import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.cmit.cmhk.entity.{UserRealTimeLabel, UserRecommendRecord}
import com.cmit.cmhk.utils.{LoadFile, Utils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.security.UserGroupInformation
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer


/**
  * Created by Chihom on 2018/03/16.
  */

class HBaseUtils extends Serializable {
  private val log = LoggerFactory.getLogger(classOf[HBaseUtils].getName)
  private val serialVersionUID = 6351075102138514681L
  var connection: Connection = null
  private val hbaseConf = HBaseConfiguration.create()
//  private val sdf1 = new SimpleDateFormat("yyyyMMdd")
//  private val sdf2 = new SimpleDateFormat("yyyy-MM-dd")
  private val userName = LoadFile.loadConfig("./cmhk-config.properties").getProperty("userName")

  def this(url: String,ugi: UserGroupInformation) {
    this()
    initKerberos(url,ugi)
  }

  def initKerberos(url: String, ugi: UserGroupInformation): Unit = {

    try {
      UserGroupInformation.setLoginUser(ugi)
      ugi.doAs(new PrivilegedExceptionAction[Unit] {
        override def run(): Unit = {
          initHBaseConnection(url)
        }
      })
    } catch {
      case ex: Exception => {
        println("HBase Kerberos Authentication Failed！" + ex.printStackTrace())
      }
    }

    def initHBaseConnection(url: String): Unit = {

      hbaseConf.set("hbase.zookeeper.quorum",url)
      hbaseConf.set("hbase.zookeeper.property.clientPort","2181")
      try {
        if (connection == null) {
          connection = ConnectionFactory.createConnection(hbaseConf)
          log.info("HBASE Connection Create Success!")
        }
      } catch {
        case ex: Exception => {
          ex.printStackTrace()
        }
      }

    }
  }

  /**
    * 漫游欢迎短信时间维度表插入方法(rowkey中发生时间在前)
    * @param str
    * @param tableName
    */
  def insertGreetingMessageTime(str: ArrayBuffer[String],tableName: String): Unit = {
    this.synchronized {
      val puts = new util.ArrayList[Put]()
      val hTable = connection.getTable(TableName.valueOf(tableName))
      try {
        for (value <- str) {
          log.debug("短信时间维度表插入内容：" + value)
          val res = genPut(value, "info")
          if (res.isDefined) {
            puts.add(res.get)
          }
        }
        if (!puts.isEmpty) {
          hTable.put(puts)
        }
      } catch {
        case ex: Exception => {
          ex.printStackTrace()
        }
      } finally {
        if (hTable != null) {
          hTable.close()
        }
      }
    }
  }

  /**
    * 漫游欢迎短信表插入方法(手机号码在前)
    * @param str
    * @param tableName
    */
  def insertGreetingMessage(str: ArrayBuffer[String],tableName: String): Unit = {
    this.synchronized {
      val puts = new util.ArrayList[Put]()
      val hTable = connection.getTable(TableName.valueOf(tableName))
      try {
        for (value <- str) {
          log.debug("短信表插入内容：" + value)
          val res = genPut(value, "info", "info")
          if (res.isDefined) {
            puts.add(res.get)
          }
        }
        if (!puts.isEmpty) {
          hTable.put(puts)
        }
      } catch {
        case ex: Exception => {
          ex.printStackTrace()
        }
      } finally {
        if (hTable != null) {
          hTable.close()
        }
      }
    }
  }

  def genPut(str: String,col: String): Option[Put] = {
    val strs = str.split("\\$",-1)
    val key = "A" + strs(0)
//    val key = strs(0)
    log.debug("插入欢迎短信时间维度表rowkey: " + key + ",值为" + strs(1))
    val put = new Put(key.getBytes)
    put.addColumn(col.getBytes(),"value".getBytes(),strs(1).getBytes())
    Some(put)
  }

  def genPut(str: String,col1: String,col2: String):Option[Put] = {

    val strs = str.split("\\$",-1)
    val rowkey = strs(0).substring(14) + strs(0).substring(0,14)
    log.debug("插入欢迎短信表rowkey: " + rowkey + ",值为" + strs(1))
    val values = strs(1).split("#",-1)
    val put = new Put(rowkey.getBytes)
    put.addColumn(col1.getBytes(),"COUNTRY_CODE".getBytes(),values(2).getBytes())
    put.addColumn(col2.getBytes(),"SM_STATUS".getBytes(),values(0).getBytes())
    put.addColumn(col2.getBytes(),"ORG_ACCOUNT".getBytes(),values(1).getBytes())
    put.addColumn(col2.getBytes(),"SEND_TIME".getBytes(),strs(0).substring(0,14).getBytes())
    Some(put)

  }

  /**
    * 查找用户预付费类型
    * @param phone
    * @return
    */

  def getRecordByRowkey(phone: String): String = {
    val sdf1 = new SimpleDateFormat("yyyyMMdd")
    var result = ""
    val startRowkey = phone + "_99998"
    val endRokey = phone + "_99999" + "~"
    val scan = new Scan(startRowkey.getBytes(),endRokey.getBytes())
    val filter1:SingleColumnValueFilter =
      new SingleColumnValueFilter(Bytes.toBytes("info"),Bytes.toBytes("DEACTIVATION_DATE"),CompareOp.GREATER_OR_EQUAL ,Bytes.toBytes(sdf1.format(new Date())))
    val filter2: SingleColumnValueFilter =
      new SingleColumnValueFilter(Bytes.toBytes("info"),Bytes.toBytes("ACTIVATION_DATE"),CompareOp.LESS_OR_EQUAL ,Bytes.toBytes(sdf1.format(new Date())))
    val filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL)
    filterList.addFilter(filter1)
    filterList.addFilter(filter2)
    scan.setFilter(filterList)
    scan.addColumn("info".getBytes(),"DEACTIVATION_DATE".getBytes())
    scan.addColumn("info".getBytes(),"ACTIVATION_DATE".getBytes())
    var scanner: ResultScanner = null
    val hTable = connection.getTable(TableName.valueOf(userName + ":RPA_MRS_PSL"))
    try {
      scanner = hTable.getScanner(scan)
      val it = scanner.iterator()
      while (it.hasNext) {
        val r = it.next()
        val rowkey = Bytes.toString(r.getRow)
        val productID = rowkey.split("_",-1)(1)

        productID match {
          case "99999" => result = productID
          case "99998" => result = productID
          case _ => None
        }
      }
    }catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if(hTable != null) {
        hTable.close()
      }
    }
    if(scanner != null) {
      scanner.close()
    }

    result
  }


  /**
    * 根据关口号查找对应的运营商国家代码
    * @param rowkey
    * @return
    */
  def getCountryCodeByADDR(rowkey: String): Option[String] = {

    val g = new Get(rowkey.getBytes())
    var value: Option[String] = None
    val hTable = connection.getTable(TableName.valueOf(userName + ":RPA_MRS_MSC_COUNTRY_CODE"))
    try {
      val result = hTable.get(g)
      if(!result.isEmpty) {
        value = Some(Bytes.toString(result.getValue("info".getBytes(),"COUNTRY_CODE".getBytes())))
      }
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    } finally {
      if(hTable != null) {
        hTable.close()
      }
    }
    value
  }

  /**
    * 将关口号及对应漫游运营商国代码存入
    * @param rowkey
    * @param countryCode
    */
  def saveCountryCode(rowkey: String,countryCode: String): Unit = {

    val put = new Put(rowkey.getBytes())
    put.addColumn("info".getBytes(),"COUNTRY_CODE".getBytes(),countryCode.getBytes())
    val hTable = connection.getTable(TableName.valueOf(userName + ":RPA_MRS_MSC_COUNTRY_CODE"))
    try {
      hTable.put(put)
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    } finally {
      if(hTable != null) {
        hTable.close()
      }
    }

  }

  /**
    * 获取用户选择的语言,0表示没找到,1表示中文，2表示英文
    * @param phone
    */
  def getUserLanguage(phone: String): Int = {
    var flag = 0
    val g = new Get(phone.getBytes)
    val hTable = connection.getTable(TableName.valueOf(userName + ":RPA_MRS_SLL"))
    try {
      val result = hTable.get(g)
      if (!result.isEmpty) {
        val language = Bytes.toString(result.getValue("info".getBytes, "LANGUAGE".getBytes))
        language.toLowerCase match {
          case "chinese" => flag = 1
          case "english" => flag = 2
          case _ => {
            None
          }
        }
      }
    } catch {
      case e : Exception => e.printStackTrace()
    } finally {
      if(hTable != null) {
        hTable.close()
      }
    }
    flag
  }

  /**
    * 根据rowkey删除某条记录
    * @param rowkey
    */
  def delete(rowkey: String): Unit = {

    val hTable = connection.getTable(TableName.valueOf(userName + ":RPA_MRS_SMS_TIME"))
    try {
      val d = new Delete(rowkey.getBytes())
      hTable.delete(d)
    } catch {
      case ex: Exception => {
        println("删除漫游欢迎短信记录失败：" + rowkey + " 错误信息为："+ ex.printStackTrace())
      }
    } finally {
      if(hTable != null) {
        hTable.close()
      }
    }
  }

  /**
    * 批量删除记录
    * @param rowkeys
    */
  def deletes(rowkeys: ArrayBuffer[String]): Unit = {

    val deletes = new util.ArrayList[Delete]()
    val hTable = connection.getTable(TableName.valueOf(userName + ":RPA_MRS_SMS_TIME"))
    try {
      for(rowkey <- rowkeys) {
        val delete = new Delete(rowkey.getBytes())
        deletes.add(delete)
      }
      hTable.delete(deletes)
    } catch {
      case ex: Exception => {
        println("批量删除漫游欢迎短信记录失败，错误信息为："+ ex.printStackTrace())
      }
    }finally {
      if(hTable != null) {
        hTable.close()
      }
    }
  }

  /**
    * 返回用户仍在生效的产品信息，包括订购生效期及激活生效期
    * @param phone_len
    * @return
    */
  def findOrderingInfo(phone_len: String): Option[Tuple2[String,String]] = {
    var products_str=""
    var set = Set[String]()
    val sdf=new SimpleDateFormat("yyyyMMdd")
    val phone=phone_len.substring(3)
    val endRowkey = phone + "_99998"
    val scan = new Scan(phone.getBytes(), endRowkey.getBytes())
    //    val limitless_fliter: SingleColumnValueFilter =
    //      new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("DEACTIVATION_DATE"), CompareOp.EQUAL, Bytes.toBytes(""))
    val greater_filter:SingleColumnValueFilter=
    new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("DEACTIVATION_DATE"), CompareOp.GREATER_OR_EQUAL,Bytes.toBytes(sdf.format(new Date())))

    //添加多个过滤器
    //    val filter_list = new FilterList(FilterList.Operator.MUST_PASS_ALL)  //既 又
    //    filter_list.addFilter(limitless_fliter) //失效时间为空，无限大
    //    filter_list.addFilter(greater_filter) //失效时间大于当前时间
    scan.setFilter(greater_filter)

    scan.addColumn("info".getBytes(), "ACTIVATION_DATE".getBytes())
    scan.addColumn("info".getBytes(), "DEACTIVATION_DATE".getBytes())
    var scanner:ResultScanner=null
    try {
      val hTable = connection.getTable(TableName.valueOf(userName + ":RPA_MRS_PSL"))
      scanner = hTable.getScanner(scan)
      /*
      查询出用户的订购记录和激活记录
       */
      var act_set:Set[String] = Set()  //存放所有生效的激活记录
      var order_records=new ArrayBuffer[(String,Int)]()   //存放所有生效的订购记录
      val it = scanner.iterator
      while (it.hasNext) {
        val s = it.next()
        val rowkey = Bytes.toString(s.getRow)
        val rowkey_array=rowkey.split("_",-1)

        /**
          * by fengzhihong 20180723 找到用户所有生效期内的订购产品记录即可
          */
        if(rowkey_array(1).length == 5 && rowkey_array(1).substring(0,2).equals("01")) {
          set += rowkey_array(1)
        } else {
          set += rowkey_array(1).substring(2)
        }
        //首先把查出来的rowkey判断为激活记录或订购记录分别放到两个分类的集合里面
//        if (rowkey_array.size>2 && rowkey_array(1).length == 5) {
//          if ( rowkey_array(1).substring(0, 2).equals("00")) {
//            //说明是一条激活记录，必然有效
//            log.info("一条激活记录：" + rowkey)
//            act_set += rowkey_array(1)   //加入到有效列表
//            val send_imsi = rowkey_array(1)
//            if(products_str=="") {
//              products_str+= send_imsi
//            }else products_str+="#"+ send_imsi
//          } else {
//            //说明是一条订购记录，添加到订购集合，需要找到
//            log.info("一条订购记录：" + rowkey)
//            val cells = s.rawCells()
//            var active_date = 0
//            for (cell <- cells) { //该rowkey下的所有列内容
//              val qualifiler = Bytes.toString(CellUtil.cloneQualifier(cell)) //列名
//            val value = Bytes.toString(CellUtil.cloneValue(cell)).toInt //列值
//              qualifiler.toLowerCase() match {
//                case "activation_date" => active_date = value
//                //                  case "deactivation_date" => deactive_date=value  //不需要
//                case _ => None
//              }
//            }
//            //将产品id和生效时间放入订购tuple
//            order_records+=new Tuple2(rowkey_array(1),active_date)
//          }
//        }
      }

      if(!set.isEmpty) {
        for(s <- set) {
          if(products_str == "") {
            if(s.substring(0,2).equals("01") && s.length == 5) {
              products_str += s
            } else {
              products_str += "00"+s+"#"+"10"+s
            }
          } else {
            if(s.substring(0,2).equals("01") && s.length == 5) {
              products_str += "#" + s
            } else {
              products_str += "#" + "00" + s + "#" + "10" + s
            }
          }
        }
      }

      //第一轮循环已完成，将符合激活记录的产品放入到字符串之中，然后针对订购产品记录，判断其是否有未筛选出来的激活记录
//      for(order <-order_records){
//        log.info("数据库中存在订购记录："+order._1+"；以及生效时间："+order._2)
//        if(order._1.substring(0,2)=="01"){ //说明是一种没有激活记录的产品
//          if(products_str=="") {
//            products_str+=order._1
//          }else products_str+="#"+ order._1
//        }else if(order._1.substring(0,1)=="1"){  //说明是预购记录
//          val order_id="0"+order._1.substring(1)
//          if(!act_set.contains(order_id)){
//            val start_key = phone + "_"+order_id+"_"+order._2
//            val end_key= phone + "_"+order_id+"_2099"
//            val act_scan = new Scan(start_key.getBytes(), end_key.getBytes())
//            //          val act_tm_filter: SingleColumnValueFilter =
//            //            new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("ACTIVATION_DATE"), CompareOp.GREATER_OR_EQUAL,Bytes.toBytes(order._2))
//            //          act_scan.setFilter(act_tm_filter)
//            //          act_scan.addColumn("info".getBytes(), "ACTIVATION_DATE".getBytes())    //scan的时候必须将过滤的列添加进来
//            //          act_scan.addColumn("info".getBytes(), "DEACTIVATION_DATE".getBytes())
//
//            val act_hTable = connection.getTable(TableName.valueOf(userName + ":RPA_MRS_PSL"))
//            scanner=null
//            scanner = act_hTable.getScanner(act_scan)
//            val act_it = scanner.iterator
//            if (!act_it.hasNext){
//              log.info(order._1+"这个订购产品不存在激活记录")
//              if(products_str=="") {
//                products_str+=order._1
//              }else products_str+="#"+order._1
//            }else{
//              log.info("该订购产品："+order._1+"存在激活记录"+Bytes.toString(act_it.next().getRow))
//            }
//          }
//        }
//      }
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
        None
      }
    } finally {
      if(scanner!=null) scanner.close() //关闭扫描器实例
    }
    Some(phone_len,products_str)

  }


  /**
    * 加载满足时间条件的欢迎短信记录(1小时之前且12小时内)
    * @return
    */
  def getHBaseList(): List[(String,String)] = {

    val startTime = System.currentTimeMillis()
    val startRowkey = "A" + Utils.getBeforeHourTime(2)
    val endRowkey = "A" + Utils.getBeforeHourTime(1)
    val filter = new RowFilter(CompareOp.GREATER_OR_EQUAL,new BinaryComparator(Bytes.toBytes(startRowkey)))
    val endFilter = new RowFilter(CompareOp.LESS_OR_EQUAL,new BinaryComparator(Bytes.toBytes(endRowkey)))
    val scan = new Scan(startRowkey.getBytes(), endRowkey.getBytes())
    scan.setFilter(filter)
    scan.setFilter(endFilter)
    scan.addColumn("info".getBytes(), "value".getBytes())
    var scanner: ResultScanner = null
    var list:List[(String,String)] = List()
    val filterArray = new ArrayBuffer[String]()
    val hTable = connection.getTable(TableName.valueOf(userName + ":RPA_MRS_SMS_TIME"))
    try {
      scanner = hTable.getScanner(scan)
      if(scanner != null) {
        val it: util.Iterator[Result] = scanner.iterator
        while (it.hasNext) {
          val s = it.next()
          val rowkey = Bytes.toString(s.getRow)
          val cells = s.rawCells()
          var qualifiler = ""
          var value = ""
          for (cell <- cells) {
            qualifiler = Bytes.toString(CellUtil.cloneQualifier(cell)) //列名
            value = Bytes.toString(CellUtil.cloneValue(cell)) //列值
          }
//          if(!filterArray.contains(rowkey)) {
//            list = list :+ (rowkey,value)
//            filterArray += rowkey
//          }
          list = list :+ (rowkey,value)
        }

      }
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    } finally {
      if(hTable != null) {
        hTable.close()
      }
      if(scanner != null) {
        scanner.close()
      }
    }
    val endTime = System.currentTimeMillis()
    log.info("读取HBASE耗时：" + (endTime - startTime))
    log.info("本批次HBASE记录数为：" + list.length)
    list.foreach(r => {
      log.info("HBase记录为：" + r)
    })
    list
  }

  /**
    * 根据国家代码、手机号码、漫游短信生产时间查询GGSN话单，判断该用户是否仍在当前国家，返回true表示离开当前国家，返回false表示仍在当前国家
    * @param countryCode
    * @param msisdn
    * @param time
    */
  def isInThisCountry(countryCode: String,msisdn: String,time: String) = {

//    var flag: Boolean = false
    var cn = ""
    var latestTime = ""
    val greater_time: SingleColumnValueFilter =
      new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("START_TIME"), CompareOp.GREATER, Bytes.toBytes(time))
//    val unequal_country: SingleColumnValueFilter =
//      new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("COUNTRY_CODE"), CompareOp.NOT_EQUAL, Bytes.toBytes(countryCode))
    val page_filter: PageFilter = new PageFilter(1) //只拿一条最新的

    //添加多个过滤器
    val filter_list = new FilterList(FilterList.Operator.MUST_PASS_ALL) //既 又
    filter_list.addFilter(greater_time)
//    filter_list.addFilter(unequal_country)
    filter_list.addFilter(page_filter)

    val scan: Scan = new Scan(msisdn.getBytes(), (msisdn + "~").getBytes())
    scan.setFilter(filter_list)

    var scanner: ResultScanner = null
    try {
      val hTable = connection.getTable(TableName.valueOf(userName + ":RPA_MRS_GGSN"))
      scanner = hTable.getScanner(scan)
      val it = scanner.iterator()
      if (it.hasNext) {  //如果当前最新的记录，国家代码不同，则表明用户已不在当前国家
//        log.info("通过ggsn查询到当前用户已经不在当前国家")
//        flag = true
        val s = it.next()
        val rowkey = Bytes.toString(s.getRow)
        val cells = s.rawCells()
        for(cell <- cells) {
          val qualifiler = Bytes.toString(CellUtil.cloneQualifier(cell)) //列名
          val value = Bytes.toString(CellUtil.cloneValue(cell))
          if(qualifiler.toUpperCase.equals("COUNTRY_CODE")) {
            cn = value
          } else if(qualifiler.toUpperCase.equals("START_TIME")) {
            latestTime = value
          }
        }
      }
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
        false
      }
    } finally {
      if (scanner != null) {
        scanner.close() //关闭扫描器实例
      }
    }
    (latestTime,cn)
  }

  /**
    * 判断是否存在用户最新欢迎短信记录，若存在则为false，返回最新记录和国家，若不存在返回true
    * @param phone
    * @param time
    * @return
    */
  def isNewestSMS(phone: String,time:String) = {

    val startKey = phone + time
    var latestTime = "" //若存在最新短信记录，保证最新短信记录时间
    var cn = "" //国家
    var flag = true //默认当前短信为最新记录
    //查询是否存在比当前欢迎短信时间更加新的记录(即目前该用户最新的短语短信记录)
    val filter = new RowFilter(CompareOp.GREATER,new BinaryComparator(Bytes.toBytes(startKey)))
    val endKey = phone + "~"
    val scan = new Scan(startKey.getBytes(),endKey.getBytes())
    scan.setFilter(filter)
    var scanner: ResultScanner = null
    var hTable: Table = null
    try {
      hTable = connection.getTable(TableName.valueOf(userName + ":RPA_MRS_SMS"))
      println(connection)
      scanner = hTable.getScanner(scan)
      val it = scanner.iterator()
      while (it.hasNext) {
        flag = false
        val s = it.next()
        var (c,l) = ("","")
        val rowkey = Bytes.toString(s.getRow)
        val cells = s.rawCells()
        for(cell <- cells) {
          val qualifiler = Bytes.toString(CellUtil.cloneQualifier(cell)) //列名
          val value = Bytes.toString(CellUtil.cloneValue(cell))
          if(qualifiler.toUpperCase.equals("COUNTRY_CODE")) {
            c = value
          } else if(qualifiler.toUpperCase.equals("SEND_TIME")) {
            l = value
          }
        }
        //保存最新的短信记录国家与时间记录
        if(l.compareTo(latestTime) > 0) {
          latestTime = l
          cn = c
        }
      }
    } catch {
      case ex: Exception => {
        log.error("查询HBase表[" + userName +":RPA_MRS_SMS]异常:" + ex)
      }
    } finally {
      if(scanner != null) {
        scanner.close()
      }
      if(hTable != null) {
        hTable.close()
      }
    }

    (flag,latestTime,cn)
  }

  /**
    * 判断某产品当天是否继续推送
    * @param rowkey
    * @param limit
    * @return
    */
  def productCountInsert(rowkey: String,limit: Long): Boolean = {

    var flag = true
    var hTable: Table = null
    try {
      hTable = connection.getTable(TableName.valueOf(userName + ":RPA_MRS_PRODUCT_LIMIT"))
      val g = new Get(rowkey.getBytes())
      val result = hTable.get(g)
      var value:Long = 0L
      if(!result.isEmpty) {
        value = Bytes.toLong(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("count")))
        log.info("当前产品：" + rowkey + " 当天已经推送数量为：" + value)
      }
      if(value >= limit) {
        flag = false
      }
    } catch {
         case ex: Exception => {
           log.error("产品记录："+rowkey+"查询表["+ userName + ":RPA_MRS_PRODUCT_LIMIT]异常" + ex )
         }
    } finally {
      if(hTable != null) {
        hTable.close()
      }
    }
    flag
  }

  /**
    * 插入用户实时位置变更表及用户实时标签记录表
    * @param userRealTimeLabel
    */
  def writeUserRealTimeLabel(userRealTimeLabel: UserRealTimeLabel): Unit = {

    var hTbale: Table = null
    var hTable1: Table = null
    try {
      hTbale = connection.getTable(TableName.valueOf(userName + ":USER_REAL_TIME_LABEL"))
      val put = new Put((userRealTimeLabel.getMsisdn+"_"+userRealTimeLabel.getLuTime).getBytes())
      put.addColumn("info".getBytes(),"MSISDN".getBytes(),userRealTimeLabel.getMsisdn.getBytes())
      put.addColumn("info".getBytes(),"IMSI".getBytes(),userRealTimeLabel.getImsi.getBytes())
      put.addColumn("info".getBytes(),"COUNTRY_CODE".getBytes(),userRealTimeLabel.getCountryCode.getBytes())
      put.addColumn("info".getBytes(),"LU_TIME".getBytes(),userRealTimeLabel.getLuTime.getBytes())
      put.addColumn("info".getBytes(),"SOURCE".getBytes(),userRealTimeLabel.getSource.getBytes())
      put.addColumn("info".getBytes(),"ORDERED_PRODUCTS_ID".getBytes(),userRealTimeLabel.getOrderProductID.getBytes())
      put.addColumn("info".getBytes(),"IS_WHITE_LIST".getBytes(),userRealTimeLabel.getIsWhiteList.getBytes())
      put.addColumn("info".getBytes(),"IS_PAY_LIST".getBytes(),userRealTimeLabel.getIsPayList.getBytes())
      hTbale.put(put)

      hTable1 = connection.getTable(TableName.valueOf(userName + ":USER_LOCATION_UPDATE"))
      val put2 = new Put((userRealTimeLabel.getMsisdn+"_"+userRealTimeLabel.getLuTime).getBytes())
      put2.addColumn("info".getBytes(),"MSISDN".getBytes(),userRealTimeLabel.getMsisdn.getBytes())
      put2.addColumn("info".getBytes(),"IMSI".getBytes(),userRealTimeLabel.getImsi.getBytes())
      put2.addColumn("info".getBytes(),"COUNTRY_CODE".getBytes(),userRealTimeLabel.getCountryCode.getBytes())
      put2.addColumn("info".getBytes(),"LU_TIME".getBytes(),userRealTimeLabel.getLuTime.getBytes())
      put2.addColumn("info".getBytes(),"LU_DATA".getBytes(),userRealTimeLabel.getLuTime.substring(0,8).getBytes())
      put2.addColumn("info".getBytes(),"SOURCE".getBytes(),userRealTimeLabel.getSource.getBytes())
      hTable1.put(put2)

    } catch {
      case ex: Exception => {
        log.error("插入表["+ userName +":USER_REAL_TIME_LABEL] or ["+ userName +":USER_LOCATION_UPDATE]异常" + ex )
      }
    } finally {
      if(hTbale != null) {
        hTbale.close()
      }
      if(hTable1 != null) {
        hTable1.close()
      }
    }

  }

  /**
    * 写用户推送记录表
    * @param map
    */
  def writeUserRecommendRecord(map:Map[String,UserRecommendRecord]): Unit = {

    var hTbale: Table = null
    try {
      hTbale = connection.getTable(TableName.valueOf(userName + ":USER_RECOMMEND_RECORD"))
      val puts = new util.ArrayList[Put]()
      for(key <- map.keys) {
        val value = map.get(key).get
        var rowkey = value.getPlanID + "_"+value.getMsisdn+"_"+value.getCountryCode+"_"+value.getLuTime+"_"+value.getProductID
        val put = new Put(rowkey.getBytes())
        put.addColumn("info".getBytes(),"PLAN_ID".getBytes(),value.getPlanID.getBytes())
        put.addColumn("info".getBytes(),"MSISDN".getBytes(),value.getMsisdn.getBytes())
        put.addColumn("info".getBytes(),"COUNTRY_CODE".getBytes(),value.getCountryCode.getBytes())
        put.addColumn("info".getBytes(),"PRODUCT_ID".getBytes(),value.getProductID.getBytes())
        put.addColumn("info".getBytes(),"LU_TIME".getBytes(),value.getLuTime.getBytes())
        put.addColumn("info".getBytes(),"PRI".getBytes(),value.getPri.getBytes())
        put.addColumn("info".getBytes(),"IS_PUSH".getBytes(),value.getIsPush.getBytes())
        puts.add(put)
      }
      if(!puts.isEmpty) {
        hTbale.put(puts)
      }

    } catch {
      case ex: Exception => {
        log.error("插入表["+ userName +":USER_RECOMMEND_RECORD]异常" + ex )
      }
    } finally {
      if(hTbale != null) {
        hTbale.close()
      }
    }

  }

  /**
    * 更新产品数量控制表记录
    * @param rowkey
    */
  def updateRecordCount(rowkey: String): Unit = {

    var hTable: Table = null
    try {
      hTable = connection.getTable(TableName.valueOf(userName + ":RPA_MRS_PRODUCT_LIMIT"))
      val g = new Get(rowkey.getBytes())
      val result = hTable.get(g)
      //更新记录
      val increment = new Increment(Bytes.toBytes(rowkey))
      increment.addColumn(Bytes.toBytes("info"),Bytes.toBytes("count"),1)
      hTable.increment(increment)

    }catch {
      case ex: Exception => {
        log.error("产品记录："+rowkey+"插入表["+ userName +":RPA_MRS_PRODUCT_LIMIT]异常" + ex )
      }
    } finally {
      if(hTable != null) {
        hTable.close()
      }
    }

  }

  /**
    * 关闭连接
    */
  def closeConnection(): Unit = {
    try
        if (connection != null) connection.close()
    catch {
      case e: SQLException =>
        log.error("关闭Phoenix Connection 连接失败：" + e)
      case e: Exception =>
        log.error("关闭Phoenix连接失败：" + e)
    }
  }

  /**
    * 校验是否满足LU过滤天数
    * @param phone
    * @param countryCode
    * @param luTime
    * @return
    */
  def checkIsPlanDay(phone: String,countryCode: String,luTime: String): Boolean = {
    var flag = true
    val startKey = phone + countryCode + luTime
    val endKey = phone + countryCode + "~"
    val filter = new RowFilter(CompareOp.GREATER,new BinaryComparator(Bytes.toBytes(startKey)))
    val scan = new Scan(startKey.getBytes(),endKey.getBytes())
    scan.setFilter(filter)
    var scanner: ResultScanner = null
    var hTable: Table = null
    try {
      hTable = connection.getTable(TableName.valueOf(userName + ":RPA_MRS_SMS_LUDAY"))
      scanner = hTable.getScanner(scan)
      val it = scanner.iterator()
      while (it.hasNext) { //LU天内存在已发短信记录
        flag = false
        it.next()
      }
    }
    flag
  }

  /**
    * 记录已存入的待发短信
    * @param phone
    * @param country
    * @param nowTime
    */
  def insertSMS(phone: String,country: String,nowTime: String): Unit = {
    val rowkey = phone + country + nowTime
    val put = new Put(rowkey.getBytes())
    put.addColumn("f".getBytes(),"v".getBytes(),"1".getBytes())
    val hTable = connection.getTable(TableName.valueOf(userName + ":RPA_MRS_SMS_LUDAY"))
    try {
      hTable.put(put)
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    } finally {
      if(hTable != null) {
        hTable.close()
      }
    }
  }


}

object HBaseUtils {

  private val log = LoggerFactory.getLogger(classOf[HBaseUtils].getName)
  var ugi: UserGroupInformation = null
  var hBaseUtils: HBaseUtils = null


  /**
    * keytabPrincipal
    * @param KerberosName mrs.ZUHU1.COM
    * @param KeyTab KeytabFile
    * @return
    */
  def initUGI(KerberosName: String,KeyTab: String): UserGroupInformation = {

    synchronized {
      var conf: Configuration = null
      if (ugi == null) {
        conf = HBaseConfiguration.create()
        UserGroupInformation.setConfiguration(conf)
        try{
          ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(KerberosName, KeyTab)
        } catch {
        case ex: Exception => {
          ex.printStackTrace()
        }
      }
    }
      //检验Kerberos认证票据是否过期
      try {
        UserGroupInformation.getLoginUser().checkTGTAndReloginFromKeytab()
      } catch {
        case ex: Exception => {
          ex.printStackTrace()
        }
      }
    }
      ugi
  }

  def getHBaseUtils(url: String,ugi: UserGroupInformation): HBaseUtils = {
//    log.debug(Thread.currentThread().getName + "获取hbaseUtil")
//    synchronized {
      if (hBaseUtils == null) {
        hBaseUtils = new HBaseUtils(url, ugi)
        log.info("{HBaseUtil.getHBaseUtils}初始化 HBase Connection 成功！")
      }
      hBaseUtils
//    }
  }


}
