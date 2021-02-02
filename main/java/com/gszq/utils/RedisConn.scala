package com.gszq.utils
import java.util
import java.util.Properties

import scala.collection.JavaConversions._
import redis.clients.jedis.{HostAndPort, JedisCluster, JedisPoolConfig}

import scala.collection.mutable
object RedisConn {

  private val pro: Properties = MyPropertiesUtil.load()
  private val poolConf = new JedisPoolConfig
  poolConf.setMaxTotal(pro.get("redis.maxTotal").toString.toInt)
  poolConf.setMaxIdle(pro.get("redis.maxIdel").toString.toInt)
  poolConf.setMaxWaitMillis(pro.get("redis.mzxWaitMillis").toString.toInt)
  val jedisClusterNodes = new java.util.HashSet[HostAndPort]()
  val hostAndPorts = pro.get("redis.hostAndPort").toString.split(",")
  for (hostAndPort <- hostAndPorts) {
    val hp = hostAndPort.split(":")
    jedisClusterNodes.add(new HostAndPort(hp(0), hp(1).toInt))
  }


  def getRedisCluster: JedisCluster = {
    new JedisCluster(jedisClusterNodes,3000,3000,5,"bigdata", poolConf)
  }

  def main(args: Array[String]): Unit = {
    val jedis = getRedisCluster
    val map: mutable.HashMap[String, String] = mutable.HashMap()
    map.put("colname1","value1")
    map.put("colname2","value2")
    map.put("colname3","value3")
    //key值为库名:表名
    jedis.hset("Db:Table:id", "name", "daniel")
    jedis.hmset("db:tb:id值",map) //传map为了避免每调redis
    val value = jedis.hget("Db:Table:id", "name")
    println(value)
  }
}
