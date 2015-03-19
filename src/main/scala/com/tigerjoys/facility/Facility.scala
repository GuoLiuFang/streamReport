package com.tigerjoys.facility

import org.apache.logging.log4j.LogManager

object Facility {
  val logger = LogManager.getLogger()
  
  
    def ipSeg(obj: Object): String = {
    val ip = String.valueOf(obj).trim()
    val index = ip.lastIndexOf(".")
    if (index != -1) {
      return ip.substring(0, index)
    } else {
      logger.info("这个IP有错误" + obj.toString())
      return "NULL"
    }
  }

}