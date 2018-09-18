package com.cmit.cmhk.mysql

import java.io.{File, FileInputStream, InputStream}
import java.util.Properties
import com.mchange.v2.c3p0.ComboPooledDataSource
import org.apache.spark.SparkFiles

/**
 * Created by Chihom on 2018/3/16.
 */
class MySQLManager(isLocal: Boolean) extends Serializable{

  private val serialVersionUID = - 643525085130526420L
  //c3p0���ӳ�������Ϣ
  private val cpds: ComboPooledDataSource = new ComboPooledDataSource(true)
  private val prop = new Properties()
  //����·������
  private var in: InputStream = _
  isLocal match {
    case true  => in = getClass.getClassLoader.getResourceAsStream("./cmhk-config.properties") //����ģʽ
    case false => in = new FileInputStream(new File(SparkFiles.get("./cmhk-config.properties"))) //��Ⱥģʽ
  }
  try {
    //���ز�����C3P0��������Ϣ
    prop.load(in)
    cpds.setJdbcUrl(prop.getProperty("mysqlUrl"))
    cpds.setDriverClass(prop.getProperty("driverClass"))
    cpds.setUser(prop.getProperty("username"))
    cpds.setPassword(prop.getProperty("password"))
    cpds.setMaxPoolSize(Integer.valueOf(prop.getProperty("maxPoolSize")))
    cpds.setMinPoolSize(Integer.valueOf(prop.getProperty("minPoolSize")))
    cpds.setAcquireIncrement(Integer.valueOf(prop.getProperty("acquireIncrement")))
    cpds.setInitialPoolSize(Integer.valueOf(prop.getProperty("initialPoolSize")))
    cpds.setMaxIdleTime(Integer.valueOf(prop.getProperty("maxIdleTime")))
    cpds.setIdleConnectionTestPeriod(60)
  } catch {
    case ex: Exception => ex.printStackTrace()
  }

  //��ȡ���ӳ�����
  def getConnection = {
    try {

      Some(cpds.getConnection())
    } catch {
      case ex: Exception => ex.printStackTrace()
        None
    }
  }

}

/**
 * ����ʱ��ɳ�ʼ��
 */
object MySQLManager {
  //����ģʽʵ��
  var mysqlManager : MySQLManager = _
  def getMySQLManager(isLocal: Boolean): MySQLManager = {
    synchronized {
      if (mysqlManager == null) {
        mysqlManager = new MySQLManager(isLocal)
      }
    }
    mysqlManager
  }


}
