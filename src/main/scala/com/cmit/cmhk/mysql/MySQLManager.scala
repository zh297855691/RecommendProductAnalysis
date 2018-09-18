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
  //c3p0连接池配置信息
  private val cpds: ComboPooledDataSource = new ComboPooledDataSource(true)
  private val prop = new Properties()
  //输入路径待定
  private var in: InputStream = _
  isLocal match {
    case true  => in = getClass.getClassLoader.getResourceAsStream("./cmhk-config.properties") //本地模式
    case false => in = new FileInputStream(new File(SparkFiles.get("./cmhk-config.properties"))) //集群模式
  }
  try {
    //加载并设置C3P0的配置信息
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

  //获取连接池连接
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
 * 编译时完成初始化
 */
object MySQLManager {
  //单例模式实现
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
