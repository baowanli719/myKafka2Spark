package com.gszq.utils

import java.sql.{Connection, PreparedStatement, ResultSet, SQLException}
import java.util.Properties

import com.alibaba.druid.pool.DruidDataSourceFactory
import javax.sql.DataSource

object MysqlConn extends Serializable {

  var dataSource: DataSource = null

  val properties: Properties = MyPropertiesUtil.load()

  try {
    val props: Properties = new Properties
    props.setProperty("url", properties.getProperty("jdbc.url"))
    props.setProperty("username", properties.getProperty("jdbc.user"))
    props.setProperty("password", properties.getProperty("jdbc.password"))
    props.setProperty("initialSize", "5") //初始化大小

    props.setProperty("maxActive", "10") //最大连接

    props.setProperty("minIdle", "5") //最小连接

    props.setProperty("maxWait", "60000") //等待时长

    props.setProperty("timeBetweenEvictionRunsMillis", "2000") //配置多久进行一次检测,检测需要关闭的连接 单位毫秒

    props.setProperty("minEvictableIdleTimeMillis", "600000") //配置连接在连接池中最小生存时间 单位毫秒

    props.setProperty("maxEvictableIdleTimeMillis", "900000") //配置连接在连接池中最大生存时间 单位毫秒

    props.setProperty("validationQuery", "select 1")
    props.setProperty("testWhileIdle", "true")
    props.setProperty("testOnBorrow", "false")
    props.setProperty("testOnReturn", "false")
    props.setProperty("keepAlive", "true")
    props.setProperty("phyMaxUseCount", "100000")
    //            props.setProperty("driverClassName", "com.mysql.jdbc.Driver");
    dataSource = DruidDataSourceFactory.createDataSource(props)
  } catch {
    case e: Exception =>
      e.printStackTrace()
  }


//  def getConnection() : Connection ={
//   try{
//     dataSource.getConnection
//   };throw SQLException
//  }

  @throws[SQLException]
  def getConnection: Connection = dataSource.getConnection

  def closeResource(resultSet: ResultSet, preparedStatement: PreparedStatement, connection: Connection): Unit = { // 关闭结果集
    // ctrl+alt+m 将java语句抽取成方法
    closeResultSet(resultSet)
    // 关闭语句执行者
    closePrepareStatement(preparedStatement)
    // 关闭连接
    closeConnection(connection)
  }

  private def closeConnection(connection: Connection): Unit = {
    if (connection != null) try
      connection.close()
    catch {
      case e: SQLException =>
        e.printStackTrace()
    }
  }

  def closePrepareStatement(preparedStatement: PreparedStatement): Unit = {
    if (preparedStatement != null) try
      preparedStatement.close()
    catch {
      case e: SQLException =>
        e.printStackTrace()
    }
  }


  def closeResultSet(resultSet: ResultSet): Unit = {
    if (resultSet != null) try
      resultSet.close()
    catch {
      case e: SQLException =>
        e.printStackTrace()
    }
  }

}
