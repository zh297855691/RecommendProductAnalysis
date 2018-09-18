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

    //���������ļ�
    PropertyConfigurator.configure(LoadFile.loadConfig("./cmhk-log.properties"))
    val log: Logger = LoggerFactory.getLogger(GreetingMessageAnalysis.getClass)
    val dateFormat1 = new SimpleDateFormat("yyyyMMddHHmmss")
    val dateFormat2 = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
    val dateFormat3 = new SimpleDateFormat("HH")

    @volatile
    private var instance: Broadcast[LoadMySQLData] = null
    private var varMap: Broadcast[(ConcurrentHashMap[String,String],ConcurrentHashMap[String,String],ConcurrentHashMap[String,String])] = null
    private var MySQLData:LoadMySQLData = null
    private var map: Map[String,String] = Map() //������µ����ʱ��
    private var timeFlag = 1

    def main(args: Array[String]): Unit = {

      val prop = LoadFile.loadConfig("./cmhk-config.properties")

      val sc = new SparkConf().setAppName("GreetingMessageAnalysis")
      val ssc = new StreamingContext(sc,Seconds(prop.getProperty("Timer").toInt))

      System.setProperty("java.security.auth.login.config", "./jaas-cache.conf")
      System.setProperty("java.security.krb5.conf", "./krb5.conf")

      /**
        * ���ü�����
        */
      val topics = prop.getProperty("TopicName")
      val topicSet = topics.trim.split(",",-1).toSet
      val brokers = prop.getProperty("KafkaBrokers")
      val groupID = prop.getProperty("GroupID")

      /**
        * ������
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
          * ������(�����Ƽ�����״̬���Ƿ���£������Ƿ����¹㲥[ÿ����ִ��])
        */
        val loadMySQLDataBroadCast = GreetingMessageAnalysis.initBroadCast(ssc.sparkContext,prop.getProperty("customerID"),true,conn)

        /**
        * ��������ÿ�춨ʱ�㲥��
        */
        val ovlBroadCast = GreetingMessageAnalysis.initBroadCast(ssc.sparkContext,conn)
        /**
          * �ر�Driver������
          */
        if(conn != null) {
          conn.close
        }
        val ugi = HBaseUtils.initUGI(prop.getProperty("KerkerosUserName"),prop.getProperty("KeyTab"))
        val hbaseConnection = HBaseUtils.getHBaseUtils(prop.getProperty("HBaseURL"),ugi)
        val hBaseRDD = ssc.sparkContext.parallelize(hbaseConnection.getHBaseList(),5)
        log.info("HBase��������¼��Ϊ��" + hBaseRDD.count() + " ��������" + hBaseRDD.getNumPartitions)
        //�ϲ�Kafka��������HBASE������
        val rdd: RDD[(String,String)] = kafkardd.union(hBaseRDD)
        log.info("Kafka��������¼��Ϊ��" + kafkardd.count() + " RDD��������" + rdd.getNumPartitions)
//        val rdd: RDD[(String,String)] = kafkardd
        //��ǰ���μ�¼��
        val count = rdd.count()
        log.info("�����δ����¼��Ϊ:" + count)
        if (count > 0) {
//          rdd.repartition(30).foreachPartition(partition => {
          rdd.foreachPartition(partition => {
          //�����ӳػ�ȡһ��Connection
          val mysqlConnection = MySQLManager.getMySQLManager(false).getConnection.get
          //��ʼ��һ��HBaseUtils����
          val ugi = HBaseUtils.initUGI(kerberosNameBroadCast.value, keyTabBroadCast.value)
          val hbaseUtils = HBaseUtils.getHBaseUtils(hBaseURLBroadCast.value, ugi)
          val greetingMessageValues = new ArrayBuffer[String]()//����ʱ����¼����
          val greetingMessageValuesAll = new ArrayBuffer[String]()//���ű��¼����
          partition.foreach(record => { //HBASE������������1Сʱ֮ǰ����ǰ12Сʱ֮�ڵ����λ�ӭ���ż�¼
            if (record._1 != null && record._1.substring(0, 1).equals("A")) {
              try {
                Utils.processMarketingSMS(record._1, hbaseUtils, customerIDBroadCast.value, loadMySQLDataBroadCast.value, ovlBroadCast.value._1, record._2,mysqlConnection)
              } catch {
                case ex: Exception => {
                  log.error("��ǰ���λ�ӭ���ż�¼Ϊ��" + record + ",����Ӫ������ʧ�ܣ�������ϢΪ��" + ex)
                }
              }
            } else { //����Kafka��������
              log.debug("����Kafka��¼Ϊ��" + record._2)
              val messages = record._2.split(",", -1)
              if(messages.length > 50) {
                //���ݻ�ӭ���ŹؿںŻ�ȡ��Ӧ����Ӫ����������
                val countryCode = Utils.getCountryCodeByADDR(messages(29).trim, ovlBroadCast.value._2, ovlBroadCast.value._3, hbaseUtils)
//                log.debug("�ü�¼��Ӧ��Ӫ�̹���Ϊ��" + countryCode)
                if ((messages(20).equals("wsm_om_num") || messages(20).equals("wsm_om")) && messages(15).equals("1")
                  && !messages(8).equals("") && !messages(5).equals("") && !messages(29).equals("")) {
                  val smsSendTime = Utils.getFormatTime(messages(8).trim)
                  val phoneNum = messages(5).trim
                  val smsType = messages(15).trim
                  val smsDescribe = messages(20).trim
                  if (!countryCode.isEmpty) {
                    //���ŷ���ʱ��+�ֻ�����+$+���ŷ���״̬+��ӭ��������+���Ҵ���
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
                    log.warn("��ǰ���λ�ӭ���ż�¼δ��ƥ�䵽��Ӫ�̹��Ҵ��룡��¼Ϊ��" + record._2)
                  }
                }
              } else {
                log.info("��ӭ���ų��Ȳ����ϣ�" + record._2)
              }
            }
          })

          if(mysqlConnection != null) {
            mysqlConnection.close()
          }
          //��ǰ��������Kafka�Ļ�ӭ������������HBASE��
          if(!greetingMessageValues.isEmpty) {
            try {
              hbaseUtils.insertGreetingMessageTime(greetingMessageValues, userName.value + ":RPA_MRS_SMS_TIME")
              hbaseUtils.insertGreetingMessage(greetingMessageValuesAll, userName.value + ":RPA_MRS_SMS")
              log.info("��¼����HBASE�ɹ�")
            } catch {
              case ex: Exception => {
                log.error("Kafka����������HBASEʧ��,����Ϊ [RPA_MRS_SMS_TIME] [RPA_MRS_SMS] ����Ϊ��" + ex)
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
    * ����ģʽ��Driver���ϻָ��������ʵ����
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
        if(map.isEmpty) { //��ʼ��
          if(!states.isEmpty) {
            for (ps: PlanState <- states) {
              map += (ps.getFlagID -> ps.getUpdateTime)
            }
          }
          //ǿ��ȫ������
          instance.unpersist(blocking)
          instance = sc.broadcast(getMySQLData(customerID, conn))
        } else {
          val flag: mutable.Map[Int,Boolean] = mutable.Map(1 -> false,2 -> false,3 -> false)
          for(ps: PlanState <- states) {
            if (map.contains(ps.getFlagID)) { //����޸�ʱ���ѱ�
              if(ps.getUpdateTime.compareTo(map.get(ps.getFlagID).get) != 0) {
                flag += (ps.getRevisionContent.toInt -> true)
                map += (ps.getFlagID -> ps.getUpdateTime)
              }
            } else { //������¼���
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
    * ��ʼ��
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
        if(dateFormat3.format(System.currentTimeMillis()).toInt == 4) { //ÿ���賿4�㣬timeFlag��Ϊ1
          timeFlag = 1
        }
        if(varMap != null && timeFlag == 1 && dateFormat3.format(System.currentTimeMillis()).toInt == 3) { //��ʱִ���㷨
          varMap.unpersist(true)
          varMap = sc.broadcast((Tools.isOVL(conn),Tools.getCountryCode(conn),Tools.getRoamingCountryCode(conn)))
          timeFlag = 0
        }
      }
    }

    varMap
  }

  /**
    * ��ʼ���㲥����
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
