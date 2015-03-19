package com.tigerjoys.processor

import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.logging.log4j.LogManager
import java.text.SimpleDateFormat
import java.util.Locale
import com.tigerjoys.facility.Facility

object Processor {
  val logger = LogManager.getLogger()
  val pattern = """^(\S+) \S+ (\S+) \S+ \[([\w\d:/]+\s[+\-]\d{4})\]  \"(.+?)\" (\d+) (\d+) \"([^\"]+)\" \"([^\"]+)\" (\S+)""".r
  def clean(line: String, broadcastVar: Option[Broadcast[Map[String, Int]]]): Tuple3[String, String, Int] = {
    var ip = "NULL"
    var time = "NULL"
    var province = -1

    val broadcast = broadcastVar.get.value
    if (StringUtils.isNotEmpty(line.trim())) {
      if (!broadcast.isEmpty) {
        val result = pattern.unapplySeq(line.trim())
        if (!result.isEmpty) {
          val tupleElementsArray = result.get.toArray
          if (!tupleElementsArray.isEmpty) {
            ip = tupleElementsArray(0)
            time = formatTime(tupleElementsArray(2))
            province = getProvince(tupleElementsArray(0), broadcast)
            new Tuple3(ip, time, province)
          } else {
            logger.info("匹配成功,结果转化出错" + line)
            new Tuple3(ip, time, province)
          }
        } else {
          logger.info("正则匹配为空！" + line)
          new Tuple3(ip, time, province)
        }

      } else {
        logger.info("广播变量为空！")
        new Tuple3(ip, time, province)
      }
    } else {
      logger.info("这行记录为空！")
      new Tuple3(ip, time, province)
    }

  } //clean

  def formatTime(time: String): String = {
    if (StringUtils.isNotEmpty(time)) {
      var formattor = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.US)
      val date = formattor.parse(time.trim());
      formattor = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      formattor.format(date)
    } else {
      logger.info("时间字符串为空！")
      "NULL"
    }

  }

  def getProvince(ip: String, broadcastVar: Map[String, Int]): Int = {
    val ipseg = Facility.ipSeg(ip)
    val result = broadcastVar.get(ipseg)
    if(!result.isEmpty){
      result.get
    }else{
      logger.info("这个ip不在IP库中" + ip)
      -1
    }
  }

}//object Processor