package com.gszq.utils

import java.io.{File, FileInputStream}
import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.apache.hadoop.security.UserGroupInformation

/**
  * 获取hbase的连接
  */

object MyHbaseUtils {
  def write2Hbase()={
      val properties: Properties = MyPropertiesUtil.load()

      //设置kerberos的relam
      System.setProperty("java.security.krb5.conf",properties.getProperty("java.security.krb5.conf"))
      val conf: Configuration = HBaseConfiguration.create()


      //读取配置文件
      val core_site: String = properties.getProperty("core-site.path")
      val hdfs_site: String = properties.getProperty("hdfs-site.path")
      val hbase_site: String = properties.getProperty("hbase-site.path")
      val coreFile = new File(core_site)
      if(!coreFile.exists()) {
        //      val in = HBaseUtil.getClass.getClassLoader.getResourceAsStream("hbase-conf/core-site.xml")
        //      configuration.addResource(in)
        val in: FileInputStream = new FileInputStream(core_site)
        conf.addResource(in)
      }
      val hdfsFile = new File(hdfs_site)
      if(!hdfsFile.exists()) {
        val in: FileInputStream = new FileInputStream(hdfs_site)
        conf.addResource(in)
      }
      val hbaseFile = new File(hbase_site)
      if(!hbaseFile.exists()) {
        val in: FileInputStream = new FileInputStream(hbase_site)
        conf.addResource(in)
      }

      //设置hbase的kerberos认证
      conf.set("hbase.zookeeper.quorum",properties.getProperty("hbase.zookeeper.quorum"))
      conf.set("hbase.zookeeper.property.clientPort",properties.getProperty("hbase.zookeeper.property.clientPort"))
      conf.set("hadoop.security.authentication",properties.getProperty("hadoop.security.authentication"))
      conf.set("hbase.security.authentication",properties.getProperty("hbase.security.authentication"))
      UserGroupInformation.setConfiguration(conf)
      UserGroupInformation.loginUserFromKeytab(properties.getProperty("hbase.regionserver.principal"),properties.getProperty("hbase.regionserver.keytab.file"))

      val connection: Connection = ConnectionFactory.createConnection(conf)


      //返回connection
      connection

  }



}
