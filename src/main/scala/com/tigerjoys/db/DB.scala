package com.tigerjoys.db

import java.sql.Connection
import java.util.Properties
import java.io.FileInputStream
import java.sql.DriverManager
import org.apache.commons.dbutils.QueryRunner
import org.apache.logging.log4j.LogManager
import scala.collection.JavaConversions._
import org.apache.commons.dbutils.handlers.ArrayListHandler
import scala.collection.mutable.Buffer
import java.io.File
import com.tigerjoys.facility.Facility

class DB {
  val logger = LogManager.getLogger()
  var result: Map[String, Int] = Map()

  def getBroadcast(): Map[String, Int] = {
    var records: Buffer[Array[Object]] = Buffer()
    //mutable.Buffer         <=>     java.util.List
    //var result = Map[String, Object]()
    val sql = "select inet_ntoa(a.ip_start),a.province_id from  ip_addresses a  order by a.ip_start"
    val run = new QueryRunner()
    val conn = getConnection
    logger.info("数据库加载成功")
    try {
      records = run.query(conn, sql, new ArrayListHandler())
      //使用ArrayListHandler()返回<List<Object[]>>
    } finally {
      conn.close()
      logger.info("断开与数据库的连接")
    }
    records.foreach(record => neat(record))
    logger.info("完成数据库数据的清洗")
    result
  }

  def neat(record: Array[Object]): Map[String, Int] = {
    val ipseg = Facility.ipSeg(record(0))
    val pro = province(record(1))
    result += (ipseg -> pro)
    return result
  }



  def province(obj: Object): Int = {
    val provin = String.valueOf(obj).trim()
    Integer.parseInt(provin)
  }

  def getConnection(): Connection = {
    val url = DB.DATABASE_URL + "://" + DB.SERVER_IP + ":" + DB.SERVER_PORT + "/" + DB.DATABASE_SID + "?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true&failOverReadOnly=false"
    Class.forName(DB.CLASS_NAME) //Driver
    DriverManager.getConnection(url, DB.USERNAME, DB.PASSWORD)
  }

} //class DB {
object DB {
  val config = new Properties()
  config.load(classOf[DB].getClassLoader().getResourceAsStream("stream.db.properties"))

  //  val in = new FileInputStream("/Users/guoliufang/Documents/workspace/streamReport/src/main/resources/stream.db.properties")
  //  val in = new FileInputStream("stream.db.properties")
  //  config.load(in)
  val CLASS_NAME = config.getProperty("CLASS_NAME");
  val DATABASE_URL = config.getProperty("DATABASE_URL");
  val SERVER_IP = config.getProperty("SERVER_IP");
  val SERVER_PORT = config.getProperty("SERVER_PORT");
  val DATABASE_SID = config.getProperty("DATABASE_SID");
  val USERNAME = config.getProperty("USERNAME");
  val PASSWORD = config.getProperty("PASSWORD");

  def main(args: Array[String]): Unit = {
    val tmp = new DB().getBroadcast
    tmp.keys.foreach(key => { print("ip字段是：" + key); println(";对应省份是：" + tmp(key)) })
  }
}