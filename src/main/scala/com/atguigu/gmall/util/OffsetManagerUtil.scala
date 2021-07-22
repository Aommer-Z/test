package com.atguigu.gmall.util

import java.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis

/**
 * @program: gmall0610
 * @description: ${description}
 * @author: zyl
 * @create: 2021-06-16 15:51
 **/
object OffsetManagerUtil {

  //将偏移量保存到redis中
  def saveOffset(topic: String, groupId: String, offsetRanges: Array[OffsetRange]): Unit = {
    //获取连接
    val jedis: Jedis = MyRedisUtil.getJedisClient()
    //拼接操作redis的key offset:topic:groupid
    var offsetKey="offset:"+topic+":"+groupId
    //定义java的map集合，用于存放每个分区对应的偏移量
    val offsetMap = new util.HashMap[String, String]()
    //对当前的offsetRangers遍历，将数据封装到offsetMap中
    for (elem <- offsetRanges) {
      val partition: Int = elem.partition
      val fromOffset: Long = elem.fromOffset
      val untilOffset: Long = elem.untilOffset
      offsetMap.put(partition.toString,untilOffset.toString)
      println("保存分区:"+partition+":"+fromOffset+"------->"+untilOffset)
    }
    jedis.hmset(offsetKey,offsetMap)
    jedis.close()
  }


  //从redis中获取偏移量
  //type:hash  key: topic:String,groupId:String field:partition  value:偏移量
  def getOffset(topic:String,groupId:String):Map[TopicPartition,Long]={
    //获取客户端连接
    val jedis: Jedis = MyRedisUtil.getJedisClient()
    //拼接操作redis的key offset:topic:groupid
    var offsetKey="offset:"+topic+":"+groupId

    //获取当前消费者组消费的主题 对应的分区以及偏移量
    val offsetMap: util.Map[String, String] = jedis.hgetAll(offsetKey)
    jedis.close()
    //将Java的map转换成scala的map
    import scala.collection.JavaConverters._
    val oMap: Map[TopicPartition, Long] = offsetMap.asScala.map {
      case (partition, offset) => {
        //Map[TopicPartition,Long]
        (new TopicPartition(topic, partition.toInt), offset.toLong)
      }
    }.toMap  //转换成不可变map

    oMap
  }

}