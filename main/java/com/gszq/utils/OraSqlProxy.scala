package com.gszq.utils

import java.sql.{Connection, PreparedStatement, ResultSet}

import org.apache.log4j.Logger


class OraSqlProxy {
  private var rs: ResultSet = _
  private var psmt: PreparedStatement = _

  /**
    * 执行修改语句
    *
    * @param conn
    * @param sql
    * @param params
    * @return
    */
  def executeUpdate(conn: Connection, sql: String, params: Array[Any]): Int = {
    var rtn = 0
    try {
      psmt = conn.prepareStatement(sql)
      if (params != null && params.length > 0) {
        for (i <- 0 until params.length) {
          psmt.setObject(i + 1, params(i))
        }
      }
      rtn = psmt.executeUpdate()
    } catch {
      case e: Exception => e.printStackTrace()
    }
    rtn
  }

  /**
    * 执行查询语句
    * 执行查询语句
    *
    * @param conn
    * @param sql
    * @return
    */
  def executeQuery(conn: Connection, sql: String) = {
    rs = null
    try {
      psmt = conn.prepareStatement(sql)
      //if (params != null && params.length > 0) {
      //  for (i <- 0 until params.length) {
      //    psmt.setObject(i + 1, params(i))
      //  }
      //}
      rs = psmt.executeQuery()
      //queryCallback.process(rs)
      psmt.close()
      //println(sql)
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  def shutdown(conn: Connection): Unit = OraConn.closeResource(rs, psmt, conn)

  def main(args: Array[String]): Unit = {

    @transient val log = Logger.getLogger("file")

    val client = OraConn.getConnection
    try {
        executeUpdate(client, "insert into test (flag) values('1')",  Array("1", "2"))

    } catch {
      case e: Exception =>
      { e.printStackTrace()
        log.error(e.printStackTrace())}
    } finally {
      shutdown(client)
    }
  }
}

