package com.cmit.cmhk

import java.sql.Connection
import java.text.SimpleDateFormat
import java.util.concurrent.ConcurrentHashMap

import com.cmit.cmhk.entity.{PlanState, SmsProfile}
import com.cmit.cmhk.hbase.HBaseUtils
import com.cmit.cmhk.mysql.MySQLManager
import com.cmit.cmhk.utils._
import kafka.serializer.StringDecoder
import org.apache.log4j.PropertyConfigurator
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.{KafkaUtils, kafkaManager}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


  /**
  * Created by fengzhihong on 2018/03/16.
  */

object GreetingMessageAnalysis {

    //加载配置文件
    PropertyConfigurator.configure(LoadFile.loadConfig("./cmhk-log.properties"))
    val log: Logger = LoggerFactory.getLogger(GreetingMessageAnalysis.getClass)
    val dateFormat1 = new SimpleDateFormat("yyyyMMddHHmmss")
    val dateFormat2 = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
    val dateFormat3 = new SimpleDateFormat("HH")

    @volatile
    private var instance: Broadcast[LoadMySQLData] = null
    private var varMap: Broadcast[(ConcurrentHashMap[String,String],ConcurrentHashMap[String,String],ConcurrentHashMap[String,String])] = null
    private var MySQLData:LoadMySQLData = null
    private var map: Map[String,String] = Map() //保存更新的最后时间
    private var timeFlag = 1

    def main(args: Array[String]): Unit = {

      val prop = LoadFile.loadConfig("./cmhk-config.properties")

      val sc = new SparkConf().setAppName("GreetingMessageAnalysis")
      val ssc = new StreamingContext(sc,Seconds(prop.getProperty("Timer").toInt))

      System.setProperty("java.security.auth.login.config", "./jaas-cache.conf")
      System.setProperty("java.security.krb5.conf", "./krb5.conf")

      /**
        * 配置加载区
        */
      val topics = prop.getProperty("TopicName")
      val topicSet = topics.trim.split(",",-1).toSet
      val brokers = prop.getProperty("KafkaBrokers")
      val groupID = prop.getProperty("GroupID")

      /**
        * 常量区
        */
      val hBaseURLBroadCast = ssc.sparkContext.broadcast(prop.getProperty("HBaseURL"))
      val kerberosNameBroadCast = ssc.sparkContext.broadcast(prop.getProperty("KerkerosUserName"))
      val keyTabBroadCast: Broadcast[String] = ssc.sparkContext.broadcast(prop.getProperty("KeyTab"))
      val customerIDBroadCast = ssc.sparkContext.broadcast(prop.getProperty("customerID"))
      val userName = ssc.sparkContext.broadcast(prop.getProperty("userName"))


      val kafkaParams = Map[String,String]("bootstrap.servers" -> brokers, "serializer.class" -> "kafka.serializer.StringEncoder",
        "group.id" -> groupID,"security.protocol" -> "PLAINTEXTSASL","sasl.mechanism" -> "GSSAPI")
//      val KafkaDStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicSet)

      val km = new kafkaManager(kafkaParams)
      val KafkaDStream = km.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicSet)

      KafkaDStream.foreachRDD(kafkardd => {

        val conn = MySQLManager.getMySQLManager(true).getConnection.get
        /**
          * 公参区(根据推荐方案状态表是否更新，决定是否重新广播[每批次执行])
        */
        val loadMySQLDataBroadCast = GreetingMessageAnalysis.initBroadCast(ssc.sparkContext,prop.getProperty("customerID"),true,conn)

        /**
        * 公参区（每天定时广播）
        */
        val ovlBroadCast = GreetingMessageAnalysis.initBroadCast(ssc.sparkContext,conn)
        /**
          * 关闭Driver端连接
          */
        if(conn != null) {
          conn.close
        }
        val ugi = HBaseUtils.initUGI(prop.getProperty("KerkerosUserName"),prop.getProperty("KeyTab"))
        val hbaseConnection = HBaseUtils.getHBaseUtils(prop.getProperty("HBaseURL"),ugi)
        val hBaseRDD = ssc.sparkContext.parallelize(hbaseConnection.getHBaseList(),5)
        log.info("HBase数据流记录数为：" + hBaseRDD.count() + " 分区数：" + hBaseRDD.getNumPartitions)
        //合并Kafka数据流与HBASE数据流
        val rdd: RDD[(String,String)] = kafkardd.union(hBaseRDD)
        log.info("Kafka数据流记录数为：" + kafkardd.count() + " RDD分区数：" + rdd.getNumPartitions)
//        val rdd: RDD[(String,String)] = kafkardd
        //当前批次记录数
        val count = rdd.count()
        log.info("本批次处理记录数为:" + count)
        if (count > 0) {
//          rdd.repartition(30).foreachPartition(partition => {
          rdd.foreachPartition(partition => {
          //从连接池获取一个Connection
          val mysqlConnection = MySQLManager.getMySQLManager(false).getConnection.get
          //初始化一个HBaseUtils对象
          val ugi = HBaseUtils.initUGI(kerberosNameBroadCast.value, keyTabBroadCast.value)
          val hbaseUtils = HBaseUtils.getHBaseUtils(hBaseURLBroadCast.value, ugi)
          val greetingMessageValues = new ArrayBuffer[String]()//短信时间表记录数组
          val greetingMessageValuesAll = new ArrayBuffer[String]()//短信表记录数组
          partition.foreach(record => { //HBASE数据流：满足1小时之前及往前12小时之内的漫游欢迎短信记录
            if (record._1 != null && record._1.substring(0, 1).equals("A")) {
              try {
                Utils.processMarketingSMS(record._1, hbaseUtils, customerIDBroadCast.value, loadMySQLDataBroadCast.value, ovlBroadCast.value._1, record._2,mysqlConnection)
              } catch {
                case ex: Exception => {
                  log.error("当前漫游欢迎短信记录为：" + record + ",生成营销短信失败，错误信息为：" + ex)
                }
              }
            } else { //来自Kafka的数据流
              log.debug("来自Kafka记录为：" + record._2)
              val messages = record._2.split(",", -1)
              if(messages.length > 50) {
                //根据欢迎短信关口号获取对应的运营商所属国家
                val countryCode = Utils.getCountryCodeByADDR(messages(29).trim, ovlBroadCast.value._2, ovlBroadCast.value._3, hbaseUtils)
//                log.debug("该记录对应运营商国家为：" + countryCode)
                if ((messages(20).equals("wsm_om_num") || messages(20).equals("wsm_om")) && messages(15).equals("1")
                  && !messages(8).equals("") && !messages(5).equals("") && !messages(29).equals("")) {
                  val smsSendTime = Utils.getFormatTime(messages(8).trim)
                  val phoneNum = messages(5).trim
                  val smsType = messages(15).trim
                  val smsDescribe = messages(20).trim
                  if (!countryCode.isEmpty) {
                    //短信发送时间+手机号码+$+短信发送状态+欢迎短信类型+国家代码
                    greetingMessageValues += (smsSendTime + phoneNum + "$" + smsType +
                      "#" + smsDescribe + "#" + countryCode.get)
//                    greetingMessageValues += ("A" + smsSendTime + phoneNum + "$" + smsType +
//                      "#" + smsDescribe + "#" + countryCode.get)
                    greetingMessageValuesAll += (smsSendTime + phoneNum + "$" + smsType +
                      "#" + smsDescribe + "#" + countryCode.get)
//                    greetingMessageValuesAll += (phoneNum + smsSendTime + "$" + smsType +
//                      "#" + smsDescribe + "#" + countryCode.get)
                  } else {
                    greetingMessageValuesAll += (smsSendTime + phoneNum + "$" + smsType +
                      "#" + smsDescribe + "#")
                    log.warn("当前漫游欢迎短信记录未能匹配到运营商国家代码！记录为：" + record._2)
                  }
                }
              } else {
                log.info("欢迎短信长度不符合：" + record._2)
              }
            }
          })

          if(mysqlConnection != null) {
            mysqlConnection.close()
          }
          //当前批次来自Kafka的欢迎短信批量插入HBASE表
          if(!greetingMessageValues.isEmpty) {
            try {
              hbaseUtils.insertGreetingMessageTime(greetingMessageValues, userName.value + ":RPA_MRS_SMS_TIME")
              hbaseUtils.insertGreetingMessage(greetingMessageValuesAll, userName.value + ":RPA_MRS_SMS")
              log.info("记录插入HBASE成功")
            } catch {
              case ex: Exception => {
                log.error("Kafka数据流插入HBASE失败,表名为 [RPA_MRS_SMS_TIME] [RPA_MRS_SMS] 错误为：" + ex)
              }
            }
          }
        })
        }
      km.updateZKOffsets(kafkardd)
    })

    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 单例模式，Driver故障恢复后可重新实例化
    * @param sc
    * @param customerID
    * @return
    */
  def initBroadCast(sc: SparkContext,customerID: String,blocking: Boolean = false,conn: Connection): Broadcast[LoadMySQLData] = {

    synchronized {
      if(instance == null) {
        instance = sc.broadcast(getMySQLData(customerID,conn))
      } else {
        val states = LoadMySQLData.getState(customerID,conn)
        if(map.isEmpty) { //初始化
          if(!states.isEmpty) {
            for (ps: PlanState <- states) {
              map += (ps.getFlagID -> ps.getUpdateTime)
            }
          }
          //强制全部更新
          instance.unpersist(blocking)
          instance = sc.broadcast(getMySQLData(customerID, conn))
        } else {
          val flag: mutable.Map[Int,Boolean] = mutable.Map(1 -> false,2 -> false,3 -> false)
          for(ps: PlanState <- states) {
            if (map.contains(ps.getFlagID)) { //最后修改时间已变
              if(ps.getUpdateTime.compareTo(map.get(ps.getFlagID).get) != 0) {
                flag += (ps.getRevisionContent.toInt -> true)
                map += (ps.getFlagID -> ps.getUpdateTime)
              }
            } else { //新增记录情况
              map += (ps.getFlagID -> ps.getUpdateTime)
              flag += (ps.getRevisionContent.toInt -> true)
            }
          }
          if(flag.get(1).get || flag.get(2).get || flag.get(3).get) {
            instance.unpersist(blocking)
          }
          if(MySQLData == null) {
            MySQLData.initValue(customerID,conn)
          } else {
            MySQLData.initValueByStates(customerID,flag,conn)
          }
          instance = sc.broadcast(MySQLData)
        }
      }
    }

    instance
  }

  /**
    * 初始化
    * @param sc
    * @param conn
    * @return
    */
  def initBroadCast(sc: SparkContext,conn: Connection): Broadcast[(ConcurrentHashMap[String,String],
    ConcurrentHashMap[String,String],ConcurrentHashMap[String,String])] = {

    if(varMap == null) {
      synchronized {
        if(varMap == null) {
          varMap = sc.broadcast((Tools.isOVL(conn),Tools.getCountryCode(conn),Tools.getRoamingCountryCode(conn)))
        }
      }
    } else {
      synchronized {
        if(dateFormat3.format(System.currentTimeMillis()).toInt == 4) { //每天凌晨4点，timeFlag置为1
          timeFlag = 1
        }
        if(varMap != null && timeFlag == 1 && dateFormat3.format(System.currentTimeMillis()).toInt == 3) { //定时执行算法
          varMap.unpersist(true)
          varMap = sc.broadcast((Tools.isOVL(conn),Tools.getCountryCode(conn),Tools.getRoamingCountryCode(conn)))
          timeFlag = 0
        }
      }
    }

    varMap
  }

  /**
    * 初始化广播对象
    * @return
    */
  def getMySQLData(customerID: String,conn: Connection): LoadMySQLData = {

    if(MySQLData == null) {
      MySQLData = LoadMySQLData.getLoadMySQLData
      MySQLData.initValue(customerID,conn)
    }

    MySQLData
  }

}
