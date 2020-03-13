package com.atguigu.gmall.realtime.util

import java.io.InputStreamReader
import java.util.Properties


object PropertiesUtil {

  /**
   * 读取当前classPath下的配置文件获取Properties对象
   * @param propertiesName 配置文件名
   * @return
   */
  def getProperties(propertiesName: String): Properties = {
    val prop: Properties = new Properties()
    prop.load(new InputStreamReader(
      Thread.currentThread().getContextClassLoader.getResourceAsStream(propertiesName),
      "UTF-8"))
    prop
  }

  def main(args: Array[String]): Unit = {
    println(PropertiesUtil.getProperties("config.properties").getProperty("kafka.broker.list"))
  }
}
