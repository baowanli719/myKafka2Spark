package com.gszq.utils

import org.apache.log4j.Logger

object ConnectOracle {


  //ora_url=jdbc:oracle:thin:@172.50.100.233:1521:test
  //ora_usr=rpt
  //ora_pwd=123456

  val user="rpt"
  val password = "123456"
  val conn_str = "jdbc:oracle:thin:@172.50.100.233:1521:test";

  println(conn_str)
  def main(args:Array[String]): Unit ={
    @transient val log = Logger.getLogger("file")
    val sqlProxy = new OraSqlProxy()
    val client = OraConn.getConnection
    try {
      sqlProxy.executeUpdate(client,"insert into test (flag) values(?)",  Array("1"))

    } catch {
      case e: Exception =>
      { e.printStackTrace()
        log.error(e.printStackTrace())}
    } finally {
      sqlProxy.shutdown(client)
    }
  }
}