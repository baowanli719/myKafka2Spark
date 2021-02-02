package com.gszq.utils

import java.io.InputStreamReader
import java.util.Properties

object MyPropertiesUtil {

  def load(): Properties = {

    val prop = new Properties()
    prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream("kafka2spark.properties")
      , "UTF-8"))

    //获取kafka2hbase.properties，默认配置路径
//    val stream: FileInputStream = new FileInputStream(System.getProperty("user.dir")+"/config/kafka2hbase.properties")
//    val reader: InputStreamReader = new InputStreamReader(stream,"UTF-8")
//    prop.load(reader)
    prop
  }
}