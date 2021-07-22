package com.atguigu.gmall.util

import java.util.Properties
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, streaming}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

/**
 * @program: sparkGmall
 * @description: 读取kafka的工具类
 * @author: zyl
 * @create: 2021-06-07 17:50
 **/
object MykafkaUtil {

  //通过工具类加载配置文件，得到一个property对象来拿到"kafka.broker.list"
  private val properties: Properties = MyPropertiesUtil.load("config.properties")
  val broker_list = properties.getProperty("kafka.broker.list")

  // kafka 消费者配置
  var kafkaParam = collection.mutable.Map(
    "bootstrap.servers" -> broker_list,//用于初始化链接到集群的地址
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    //用于标识这个消费者属于哪个消费团体
    "group.id" -> "gmall0612_group",
    //latest 自动重置偏移量为最新的偏移量
    "auto.offset.reset" -> "latest",
    //如果是 true，则这个消费者的偏移量会在后台自动提交,但是 kafka 宕机容易丢失数据
    //如果是 false，会需要手动维护 kafka 偏移量
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )


  // 创建 DStream，返回接收到的输入数据 消费kafka数据时，采用默认的消费者组
  def getKafkaStream(topic: String,ssc:StreamingContext ): InputDStream[ConsumerRecord[String,String]]={
    val dStream = KafkaUtils.createDirectStream[String,String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](Array(topic), kafkaParam )
    )
    dStream
  }
  //在堆kafka数据进行消费时，指定消费者组，没用默认的消费者组
  def getKafkaStream(topic: String,ssc:StreamingContext,groupId:String):
  InputDStream[ConsumerRecord[String,String]]={ //ConsumerRecord[String,String]是对kafka对象的封装，k其实就是分区，v才是数据
    kafkaParam("group.id")=groupId
    val dStream = KafkaUtils.createDirectStream[String,String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](Array(topic),kafkaParam ))
    dStream
  }
  //从指定的偏移量位置读取数据
  //offsets:Map[TopicPartition,Long] 我们要保证精准一次性消费，就需要我们自己维护偏移量，所以下次读取的时候要从自己维护的偏移量的位置读取
  //所以在读偏移量的时候，偏移量要包括哪个主题的哪个分区的什么位置
  def getKafkaStream(topic: String,ssc:StreamingContext,offsets:Map[TopicPartition,Long],groupId:String)
  : InputDStream[ConsumerRecord[String,String]]={
    kafkaParam("group.id")=groupId
    val dStream = KafkaUtils.createDirectStream[String,String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](Array(topic),kafkaParam,offsets))
    dStream
  }

}
