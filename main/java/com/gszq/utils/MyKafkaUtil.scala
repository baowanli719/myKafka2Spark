package com.gszq.utils

import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import scala.collection.mutable

object MyKafkaUtil {
  private val properties: Properties = MyPropertiesUtil.load()

  //读取kafka_topics
  val topics: Array[String] = properties.getProperty("kafka_topics").split(",")
  //val topics: Array[String] = Array(properties.getProperty("kafka_topics"))

  //设置kafka连接属性
  val kafkaPara = Map[String,Object](
    "bootstrap.servers" -> properties.getProperty("bootstrap.servers"),
    "group.id" -> properties.getProperty("group.id"),
    //"zookeeper.connect"->"39.100.21.165:2181",
    //"security.protocol" -> properties.getProperty("security.protocol"),
    //"sasl.kerberos.service.name" -> properties.getProperty("sasl.kerberos.service.name"),
    //"sasl.mechanism" -> "GSSAPI",
    // "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
    "key.deserializer" -> classOf[StringDeserializer],
    // "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    "value.deserializer" -> classOf[StringDeserializer],
    "auto.offset.reset" -> "earliest",
    "enable.auto.commit" -> (false:java.lang.Boolean)
  )


  //手动维护offset方式
  def getKafkaStream2(ssc: StreamingContext, offsetMap: mutable.HashMap[TopicPartition,
    Long]) = {
    val kafkaDSteam: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String,String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](topics, kafkaPara,offsetMap))
    kafkaDSteam
  }


  //自动提交offset方式
  def getKafkaStream(ssc: StreamingContext)= {
    //基于Direct方式消费Kafka数据
    //val kafkaDStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder,
    //StringDecoder](ssc, kafkaPara, topics)
    val kafkaDSteam: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String,String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](topics, kafkaPara))


    kafkaDSteam
  }

}
