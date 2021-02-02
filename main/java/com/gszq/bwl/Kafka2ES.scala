package com.gszq.bwl

import java.sql.ResultSet
import java.text.SimpleDateFormat
import java.util
import java.util.{Date, Properties}

import com.alibaba.fastjson.JSON
import com.gszq.utils._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object Kafka2ES {
//
//  @transient val log = Logger.getLogger("file")
//  //log.info("hello scala log4j")
//  def main(args: Array[String]): Unit = {


//    //获取配置文件对象
//    val properties: Properties = MyPropertiesUtil.load()
//    val nameSpace: String =properties.getProperty("namespace")
//    val timeInterval: Int = properties.getProperty("time.interval").toInt
//    val topics: Array[String] = properties.getProperty("kafka_topics").split(",")
//    //val table_relation: String = properties.getProperty("table_relation")
//    //val tables: Array[String] = properties.getProperty("storage.table").split(",")
//    //val trJS: JSONObject = JSON.parseObject(table_relation)
//
//    //System.setProperty("java.security.auth.login.config", properties.getProperty("java.security.auth.login.config"));
//    //System.setProperty("java.security.krb5.conf", properties.getProperty("java.security.krb5.conf"));
//
//
//    //1、创建sparkconf
//    val conf = new SparkConf()
//      .setAppName("scala_bwl_kafka2ES")
//      //.setMaster(properties.getProperty("spark.master"))
//      .set("spark.streaming.kafka.maxRatePerPartition", properties.getProperty("maxRatePerPartition"))
//      .set("spark.streaming.backpressure.enabled", "true")
//      .set("spark.streaming.kafka.consumer.poll.ms", "10000")
//    //      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
//    //.set("spark.max.cores",properties.getProperty("spark.max.cores"))
//    //.set("spark.driver.allowMultipleContexts","true")
//
//    val ssc: StreamingContext = new StreamingContext(conf,Seconds(timeInterval))
//    //    val id: String = ssc.sparkContext.applicationId
//    //    PIDBean.setPID(id)
//    //
//    //    log.error("-----------------------------id:"+id+"---------------------------")
//
//    //val sparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
//
//    //读取kafka数据
//    //val ds1: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(ssc)
//
//
//    //获取offset
//    //查询mysql中是否有偏移量
//    val sqlProxy = new MysqlSqlProxy()
//    val offsetMap = new mutable.HashMap[TopicPartition, Long]()
//    val client = MysqlConn.getConnection
//    val groupid: String = properties.getProperty("group.id")
//    try {
//      for(topic<-topics){
//        sqlProxy.executeQuery(client, "select * from `"+properties.getProperty("offset_table")+"` where group_id=? and " +
//          "topic=?",
//          Array(groupid,topic), new myQueryCallback {
//            override def process(rs: ResultSet): Unit = {
//              while (rs.next()) {
//                val model = new TopicPartition(rs.getString(2), rs.getInt(3))
//                val offset = rs.getLong(4)
//                offsetMap.put(model, offset)
//              }
//              rs.close() //关闭游标
//            }
//          })
//      }
//    } catch {
//      case e: Exception => e.printStackTrace()
//    } finally {
//      sqlProxy.shutdown(client)
//    }
//
//    //判断是否有数据，如有，根据偏移量进行消费
//    val ds1: InputDStream[ConsumerRecord[String, String]] = if (offsetMap.isEmpty){
//      // println(1)
//      MyKafkaUtil.getKafkaStream(ssc)
//    }else{
//      //println(2)
//      MyKafkaUtil.getKafkaStream2(ssc,offsetMap)
//    }
//
//    //执行业务操作
//
//    val tablename_rule1 = Array("STOCKREAL", "CRDTSTOCKREAL", "SECUMREAL")
//    val tablename_rule2 = Array("BANKTRANSFER", "FUNDJOUR")
//    val tablename_rule3 = Array("COMPACT")
//    val tablename_rule4 = Array("BANKTRANSFER")
//    val entrust_bs_rule = Array("1", "2")
//    //委托种类
//    val exchange_type_rule = Array("1", "2")
//    //交易类别
//    val branch_no_rule = Array("8888", "9800", "9900")
//    //自营机构
//    val real_status_rule = Array("0", "4")
//    //处理标志
//    val real_type_rule1 = Array("0")
//    //成交类型
//    val real_type_rule2 = Array("6", "7", "8", "9")
//    val stock_type_rule = Array("0", "1", "d", "c", "h", "e", "g", "D", "L", "6", "T", "p", "q")
//    //证券类别
//    val operType_rule = Array("I")
//    val operType_rule2 = Array("I", "U")
//    //operType  D：delete;I:insert;U:update:DT:truncate;
//    val trans_type_rule = Array("01", "02")
//    //转账类型
//    val money_type_rule = Array("0", "1")
//    //货币代码
//    val bktrans_status_rule = Array("2")
//    //转账状
//    val asset_prop_rule = Array("0")
//    //账户属性
//    val business_flag_rule = Array("2041", "2042", "2141", "2142") //业务品种
//
//    ds1.foreachRDD(rddKafka=>{
//      val startTime: Long = System.currentTimeMillis
//      log.error("-----------------startTime:"+startTime+"---------------------------")
//      //println(1)
//      //val rdd: RDD[ConsumerRecord[String, String]] = rddKafka.cache()
//      val fm = new SimpleDateFormat("yyyy-MM-dd")
//      val curr_date: String = fm.format(startTime)
//
//      rddKafka.foreachPartition(partitionRecords => {
//
//
//        val list_stockreal = new util.ArrayList[util.Map[String, AnyRef]]
//
//        partitionRecords.foreach(line => {
//          //将Kafka的每一条消息解析为JSON格式数据
//          val jsonObj =  JSON.parseObject(line.value())
//          //println(line.value())
//
//          val operType = jsonObj.getString("operType").toUpperCase();
//          val tablename = jsonObj.getString("Tablename");
//          val timeload = jsonObj.getString("timeload");
//          val d = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//          val date = new Date
//
//          //val jsonObj2 = null
//
//          val jsonObj2 = JSON.parseObject(jsonObj.getString("columnInfo"))
//
//
//
//          if (tablename_rule2.contains(tablename) && operType_rule2.contains(operType)) {
//              val fund_account = jsonObj2.getString("FUND_ACCOUNT")
//              val money_type = jsonObj2.getString("MONEY_TYPE")
//              val business_flag = jsonObj2.getString("BUSINESS_FLAG")
//              val occur_balance = jsonObj2.getString("OCCUR_BALANCE")
//              val rowkey = jsonObj2.getString("POSITION_STR")
//              if (money_type_rule.contains(money_type) && business_flag_rule.contains(business_flag)) {
//                val map1 = new util.HashMap[String, AnyRef]
//                map1.put("position_str", rowkey)
//                map1.put("money_type", money_type)
//                map1.put("fund_account", fund_account)
//                map1.put("columnValue", occur_balance)
//                map1.put("time_load", timeload)
//                map1.put("index", "hs_asset_fundjour")
//                map1.put("init_date", jsonObj2.getString("INIT_DATE"))
//                list_stockreal.add(map1)
//              }
//            }
//            else if (tablename_rule1.contains(tablename) && operType_rule2.contains(operType)) {
//              val fund_account = jsonObj2.getString("FUND_ACCOUNT")
//              val money_type = jsonObj2.getString("MONEY_TYPE")
//              val occur_balance = "0"
//              val rowkey = jsonObj2.getString("POSITION_STR")
//              val map1 = new util.HashMap[String, AnyRef]
//              map1.put("position_str", rowkey)
//              map1.put("money_type", money_type)
//              map1.put("fund_account", fund_account)
//              map1.put("columnValue", occur_balance)
//              map1.put("time_load", timeload)
//              map1.put("init_date", jsonObj2.getString("INIT_DATE"))
//              if (tablename == "STOCKREAL") map1.put("index", "hs_secu_stockreal")
//              else if (tablename == "CRDTSTOCKREAL") map1.put("index", "hs_crdt_crdtstockreal")
//              else if (tablename == "SECUMREAL") map1.put("index", "hs_prod_secumreal")
//              list_stockreal.add(map1)
//            }
//            else if (tablename_rule3.contains(tablename) && operType_rule2.contains(operType)) {
//              val fund_account = jsonObj2.getString("FUND_ACCOUNT")
//              val money_type = jsonObj2.getString("MONEY_TYPE")
//              val occur_balance = "0"
//              val rowkey = jsonObj2.getString("COMPACT_ID")
//              val map1 = new util.HashMap[String, AnyRef]
//              map1.put("position_str", rowkey)
//              map1.put("money_type", money_type)
//              map1.put("fund_account", fund_account)
//              map1.put("columnValue", occur_balance)
//              map1.put("time_load", timeload)
//              map1.put("init_date", jsonObj2.getString("INIT_DATE"))
//              map1.put("index", "hs_crdt_compact")
//              list_stockreal.add(map1)
//            }
//            else {
//              //System.out.println("************************ not match:" + res1.values() + "**************************");
//            }
//
//        try {
//          //insertMany(hbtable,list);
//          if (list_stockreal.size() > 0) {
//
//            //insertDetail(list_fundjour, dates,"realtime_bank_transfer");
//            //insertDetail2Hbase(list_fundjour,"RL_ARM:BANKTRANSFER_FUNDJOUR");
//
//            MyEsTools.insertBatch(list_stockreal);
//            //MyUtils.insertDetail2Oracle(list_stockreal, dates,"hs_asset_fundjour");
//            //System.out.println(list_stockreal);
//
//          } else {
//            System.out.println("没有符合条件的数");
//          }
//        } catch {
//          case e: Exception => e.printStackTrace()
//        }
//
//
//        })
//        //connection.close()
//
//      })
//
//      val sqlProxy = new MysqlSqlProxy()
//      val client = MysqlConn.getConnection
//      try {
//        val offsetRanges: Array[OffsetRange] = rddKafka.asInstanceOf[HasOffsetRanges].offsetRanges
//        for (or <- offsetRanges) {
//          sqlProxy.executeUpdate(client, "replace into `"+properties.getProperty("offset_table")+"` (group_id,topic," +
//            "`partitions`, Offset) values(?,?,?,?)",
//            Array(groupid, or.topic, or.partition.toString, or.untilOffset))
//        }
//      } catch {
//        case e: Exception =>
//        { e.printStackTrace()
//          log.error(e.printStackTrace())}
//      } finally {
//        sqlProxy.shutdown(client)
//      }
//    })
//
//
//    //处理完 业务逻辑后 手动提交offset维护到mysql中
//    /*ds1.foreachRDD(rdd => {
//      val sqlProxy = new SqlProxy()
//      val client = DataSourceUtil.getConnection
//      try {
//        val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
//        for (or <- offsetRanges) {
//          sqlProxy.executeUpdate(client, "replace into `"+properties.getProperty("offset_table")+"` (group_id,topic," +
//            "`partitions`, Offset) values(?,?,?,?)",
//            Array(groupid, or.topic, or.partition.toString, or.untilOffset))
//        }
//      } catch {
//        case e: Exception =>
//        { e.printStackTrace()
//          log.error(e.printStackTrace())}
//      } finally {
//        sqlProxy.shutdown(client)
//      }
//    })
//    */
//
//    ssc.start()
//    ssc.awaitTermination()
//  }
}
