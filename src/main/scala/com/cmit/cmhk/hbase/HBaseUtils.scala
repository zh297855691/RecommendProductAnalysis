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
        println("HBase Kerberos Authentication Failed��" + ex.printStackTrace())
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
    * ���λ�ӭ����ʱ��ά�ȱ���뷽��(rowkey�з���ʱ����ǰ)
    * @param str
    * @param tableName
    */
  def insertGreetingMessageTime(str: ArrayBuffer[String],tableName: String): Unit = {
    this.synchronized {
      val puts = new util.ArrayList[Put]()
      val hTable = connection.getTable(TableName.valueOf(tableName))
      try {
        for (value <- str) {
          log.debug("����ʱ��ά�ȱ�������ݣ�" + value)
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
    * ���λ�ӭ���ű���뷽��(�ֻ�������ǰ)
    * @param str
    * @param tableName
    */
  def insertGreetingMessage(str: ArrayBuffer[String],tableName: String): Unit = {
    this.synchronized {
      val puts = new util.ArrayList[Put]()
      val hTable = connection.getTable(TableName.valueOf(tableName))
      try {
        for (value <- str) {
          log.debug("���ű�������ݣ�" + value)
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
    log.debug("���뻶ӭ����ʱ��ά�ȱ�rowkey: " + key + ",ֵΪ" + strs(1))
    val put = new Put(key.getBytes)
    put.addColumn(col.getBytes(),"value".getBytes(),strs(1).getBytes())
    Some(put)
  }

  def genPut(str: String,col1: String,col2: String):Option[Put] = {

    val strs = str.split("\\$",-1)
    val rowkey = strs(0).substring(14) + strs(0).substring(0,14)
    log.debug("���뻶ӭ���ű�rowkey: " + rowkey + ",ֵΪ" + strs(1))
    val values = strs(1).split("#",-1)
    val put = new Put(rowkey.getBytes)
    put.addColumn(col1.getBytes(),"COUNTRY_CODE".getBytes(),values(2).getBytes())
    put.addColumn(col2.getBytes(),"SM_STATUS".getBytes(),values(0).getBytes())
    put.addColumn(col2.getBytes(),"ORG_ACCOUNT".getBytes(),values(1).getBytes())
    put.addColumn(col2.getBytes(),"SEND_TIME".getBytes(),strs(0).substring(0,14).getBytes())
    Some(put)

  }

  /**
    * �����û�Ԥ��������
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
    * ���ݹؿںŲ��Ҷ�Ӧ����Ӫ�̹��Ҵ���
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
    * ���ؿںż���Ӧ������Ӫ�̹��������
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
    * ��ȡ�û�ѡ�������,0��ʾû�ҵ�,1��ʾ���ģ�2��ʾӢ��
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
    * ����rowkeyɾ��ĳ����¼
    * @param rowkey
    */
  def delete(rowkey: String): Unit = {

    val hTable = connection.getTable(TableName.valueOf(userName + ":RPA_MRS_SMS_TIME"))
    try {
      val d = new Delete(rowkey.getBytes())
      hTable.delete(d)
    } catch {
      case ex: Exception => {
        println("ɾ�����λ�ӭ���ż�¼ʧ�ܣ�" + rowkey + " ������ϢΪ��"+ ex.printStackTrace())
      }
    } finally {
      if(hTable != null) {
        hTable.close()
      }
    }
  }

  /**
    * ����ɾ����¼
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
        println("����ɾ�����λ�ӭ���ż�¼ʧ�ܣ�������ϢΪ��"+ ex.printStackTrace())
      }
    }finally {
      if(hTable != null) {
        hTable.close()
      }
    }
  }

  /**
    * �����û�������Ч�Ĳ�Ʒ��Ϣ������������Ч�ڼ�������Ч��
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

    //��Ӷ��������
    //    val filter_list = new FilterList(FilterList.Operator.MUST_PASS_ALL)  //�� ��
    //    filter_list.addFilter(limitless_fliter) //ʧЧʱ��Ϊ�գ����޴�
    //    filter_list.addFilter(greater_filter) //ʧЧʱ����ڵ�ǰʱ��
    scan.setFilter(greater_filter)

    scan.addColumn("info".getBytes(), "ACTIVATION_DATE".getBytes())
    scan.addColumn("info".getBytes(), "DEACTIVATION_DATE".getBytes())
    var scanner:ResultScanner=null
    try {
      val hTable = connection.getTable(TableName.valueOf(userName + ":RPA_MRS_PSL"))
      scanner = hTable.getScanner(scan)
      /*
      ��ѯ���û��Ķ�����¼�ͼ����¼
       */
      var act_set:Set[String] = Set()  //���������Ч�ļ����¼
      var order_records=new ArrayBuffer[(String,Int)]()   //���������Ч�Ķ�����¼
      val it = scanner.iterator
      while (it.hasNext) {
        val s = it.next()
        val rowkey = Bytes.toString(s.getRow)
        val rowkey_array=rowkey.split("_",-1)

        /**
          * by fengzhihong 20180723 �ҵ��û�������Ч���ڵĶ�����Ʒ��¼����
          */
        if(rowkey_array(1).length == 5 && rowkey_array(1).substring(0,2).equals("01")) {
          set += rowkey_array(1)
        } else {
          set += rowkey_array(1).substring(2)
        }
        //���ȰѲ������rowkey�ж�Ϊ�����¼�򶩹���¼�ֱ�ŵ���������ļ�������
//        if (rowkey_array.size>2 && rowkey_array(1).length == 5) {
//          if ( rowkey_array(1).substring(0, 2).equals("00")) {
//            //˵����һ�������¼����Ȼ��Ч
//            log.info("һ�������¼��" + rowkey)
//            act_set += rowkey_array(1)   //���뵽��Ч�б�
//            val send_imsi = rowkey_array(1)
//            if(products_str=="") {
//              products_str+= send_imsi
//            }else products_str+="#"+ send_imsi
//          } else {
//            //˵����һ��������¼����ӵ��������ϣ���Ҫ�ҵ�
//            log.info("һ��������¼��" + rowkey)
//            val cells = s.rawCells()
//            var active_date = 0
//            for (cell <- cells) { //��rowkey�µ�����������
//              val qualifiler = Bytes.toString(CellUtil.cloneQualifier(cell)) //����
//            val value = Bytes.toString(CellUtil.cloneValue(cell)).toInt //��ֵ
//              qualifiler.toLowerCase() match {
//                case "activation_date" => active_date = value
//                //                  case "deactivation_date" => deactive_date=value  //����Ҫ
//                case _ => None
//              }
//            }
//            //����Ʒid����Чʱ����붩��tuple
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

      //��һ��ѭ������ɣ������ϼ����¼�Ĳ�Ʒ���뵽�ַ���֮�У�Ȼ����Զ�����Ʒ��¼���ж����Ƿ���δɸѡ�����ļ����¼
//      for(order <-order_records){
//        log.info("���ݿ��д��ڶ�����¼��"+order._1+"���Լ���Чʱ�䣺"+order._2)
//        if(order._1.substring(0,2)=="01"){ //˵����һ��û�м����¼�Ĳ�Ʒ
//          if(products_str=="") {
//            products_str+=order._1
//          }else products_str+="#"+ order._1
//        }else if(order._1.substring(0,1)=="1"){  //˵����Ԥ����¼
//          val order_id="0"+order._1.substring(1)
//          if(!act_set.contains(order_id)){
//            val start_key = phone + "_"+order_id+"_"+order._2
//            val end_key= phone + "_"+order_id+"_2099"
//            val act_scan = new Scan(start_key.getBytes(), end_key.getBytes())
//            //          val act_tm_filter: SingleColumnValueFilter =
//            //            new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("ACTIVATION_DATE"), CompareOp.GREATER_OR_EQUAL,Bytes.toBytes(order._2))
//            //          act_scan.setFilter(act_tm_filter)
//            //          act_scan.addColumn("info".getBytes(), "ACTIVATION_DATE".getBytes())    //scan��ʱ����뽫���˵�����ӽ���
//            //          act_scan.addColumn("info".getBytes(), "DEACTIVATION_DATE".getBytes())
//
//            val act_hTable = connection.getTable(TableName.valueOf(userName + ":RPA_MRS_PSL"))
//            scanner=null
//            scanner = act_hTable.getScanner(act_scan)
//            val act_it = scanner.iterator
//            if (!act_it.hasNext){
//              log.info(order._1+"���������Ʒ�����ڼ����¼")
//              if(products_str=="") {
//                products_str+=order._1
//              }else products_str+="#"+order._1
//            }else{
//              log.info("�ö�����Ʒ��"+order._1+"���ڼ����¼"+Bytes.toString(act_it.next().getRow))
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
      if(scanner!=null) scanner.close() //�ر�ɨ����ʵ��
    }
    Some(phone_len,products_str)

  }


  /**
    * ��������ʱ�������Ļ�ӭ���ż�¼(1Сʱ֮ǰ��12Сʱ��)
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
            qualifiler = Bytes.toString(CellUtil.cloneQualifier(cell)) //����
            value = Bytes.toString(CellUtil.cloneValue(cell)) //��ֵ
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
    log.info("��ȡHBASE��ʱ��" + (endTime - startTime))
    log.info("������HBASE��¼��Ϊ��" + list.length)
    list.foreach(r => {
      log.info("HBase��¼Ϊ��" + r)
    })
    list
  }

  /**
    * ���ݹ��Ҵ��롢�ֻ����롢���ζ�������ʱ���ѯGGSN�������жϸ��û��Ƿ����ڵ�ǰ���ң�����true��ʾ�뿪��ǰ���ң�����false��ʾ���ڵ�ǰ����
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
    val page_filter: PageFilter = new PageFilter(1) //ֻ��һ�����µ�

    //��Ӷ��������
    val filter_list = new FilterList(FilterList.Operator.MUST_PASS_ALL) //�� ��
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
      if (it.hasNext) {  //�����ǰ���µļ�¼�����Ҵ��벻ͬ��������û��Ѳ��ڵ�ǰ����
//        log.info("ͨ��ggsn��ѯ����ǰ�û��Ѿ����ڵ�ǰ����")
//        flag = true
        val s = it.next()
        val rowkey = Bytes.toString(s.getRow)
        val cells = s.rawCells()
        for(cell <- cells) {
          val qualifiler = Bytes.toString(CellUtil.cloneQualifier(cell)) //����
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
        scanner.close() //�ر�ɨ����ʵ��
      }
    }
    (latestTime,cn)
  }

  /**
    * �ж��Ƿ�����û����»�ӭ���ż�¼����������Ϊfalse���������¼�¼�͹��ң��������ڷ���true
    * @param phone
    * @param time
    * @return
    */
  def isNewestSMS(phone: String,time:String) = {

    val startKey = phone + time
    var latestTime = "" //���������¶��ż�¼����֤���¶��ż�¼ʱ��
    var cn = "" //����
    var flag = true //Ĭ�ϵ�ǰ����Ϊ���¼�¼
    //��ѯ�Ƿ���ڱȵ�ǰ��ӭ����ʱ������µļ�¼(��Ŀǰ���û����µĶ�����ż�¼)
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
          val qualifiler = Bytes.toString(CellUtil.cloneQualifier(cell)) //����
          val value = Bytes.toString(CellUtil.cloneValue(cell))
          if(qualifiler.toUpperCase.equals("COUNTRY_CODE")) {
            c = value
          } else if(qualifiler.toUpperCase.equals("SEND_TIME")) {
            l = value
          }
        }
        //�������µĶ��ż�¼������ʱ���¼
        if(l.compareTo(latestTime) > 0) {
          latestTime = l
          cn = c
        }
      }
    } catch {
      case ex: Exception => {
        log.error("��ѯHBase��[" + userName +":RPA_MRS_SMS]�쳣:" + ex)
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
    * �ж�ĳ��Ʒ�����Ƿ��������
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
        log.info("��ǰ��Ʒ��" + rowkey + " �����Ѿ���������Ϊ��" + value)
      }
      if(value >= limit) {
        flag = false
      }
    } catch {
         case ex: Exception => {
           log.error("��Ʒ��¼��"+rowkey+"��ѯ��["+ userName + ":RPA_MRS_PRODUCT_LIMIT]�쳣" + ex )
         }
    } finally {
      if(hTable != null) {
        hTable.close()
      }
    }
    flag
  }

  /**
    * �����û�ʵʱλ�ñ�����û�ʵʱ��ǩ��¼��
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
        log.error("�����["+ userName +":USER_REAL_TIME_LABEL] or ["+ userName +":USER_LOCATION_UPDATE]�쳣" + ex )
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
    * д�û����ͼ�¼��
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
        log.error("�����["+ userName +":USER_RECOMMEND_RECORD]�쳣" + ex )
      }
    } finally {
      if(hTbale != null) {
        hTbale.close()
      }
    }

  }

  /**
    * ���²�Ʒ�������Ʊ��¼
    * @param rowkey
    */
  def updateRecordCount(rowkey: String): Unit = {

    var hTable: Table = null
    try {
      hTable = connection.getTable(TableName.valueOf(userName + ":RPA_MRS_PRODUCT_LIMIT"))
      val g = new Get(rowkey.getBytes())
      val result = hTable.get(g)
      //���¼�¼
      val increment = new Increment(Bytes.toBytes(rowkey))
      increment.addColumn(Bytes.toBytes("info"),Bytes.toBytes("count"),1)
      hTable.increment(increment)

    }catch {
      case ex: Exception => {
        log.error("��Ʒ��¼��"+rowkey+"�����["+ userName +":RPA_MRS_PRODUCT_LIMIT]�쳣" + ex )
      }
    } finally {
      if(hTable != null) {
        hTable.close()
      }
    }

  }

  /**
    * �ر�����
    */
  def closeConnection(): Unit = {
    try
        if (connection != null) connection.close()
    catch {
      case e: SQLException =>
        log.error("�ر�Phoenix Connection ����ʧ�ܣ�" + e)
      case e: Exception =>
        log.error("�ر�Phoenix����ʧ�ܣ�" + e)
    }
  }

  /**
    * У���Ƿ�����LU��������
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
      while (it.hasNext) { //LU���ڴ����ѷ����ż�¼
        flag = false
        it.next()
      }
    }
    flag
  }

  /**
    * ��¼�Ѵ���Ĵ�������
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
      //����Kerberos��֤Ʊ���Ƿ����
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
//    log.debug(Thread.currentThread().getName + "��ȡhbaseUtil")
//    synchronized {
      if (hBaseUtils == null) {
        hBaseUtils = new HBaseUtils(url, ugi)
        log.info("{HBaseUtil.getHBaseUtils}��ʼ�� HBase Connection �ɹ���")
      }
      hBaseUtils
//    }
  }


}
