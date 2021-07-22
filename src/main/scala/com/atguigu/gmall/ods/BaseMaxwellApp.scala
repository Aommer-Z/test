package com.atguigu.gmall.ods

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.atguigu.gmall.util.{MykafkaSink, MykafkaUtil, OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @program: gmall0610
 * @description: ${description}
 * @author: zyl
 * @create: 2021-06-17 15:53
 **/
object BaseMaxwellApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("BaseMaxwellApp").setMaster("local[4]")
    val ssc = new StreamingContext(conf, Seconds(5))

    var topic = "gmall_maxwel"
    var groupId = "base_db_maxwell_group"
    var recordDstream: InputDStream[ConsumerRecord[String, String]]=null

    //从redis中获取偏移量
    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)
    if (offsetMap!=null && offsetMap.size>0){
      recordDstream = MykafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupId)
    }else{
      recordDstream = MykafkaUtil.getKafkaStream(topic, ssc, groupId)
    }

    var offsetRanges: Array[OffsetRange]=null
    //获取当前采集周期中读取的主题对应的分区以及偏移量
    val offsetDstream: DStream[ConsumerRecord[String, String]] = recordDstream.transform {
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }

    //对接收到的数据进行结构转换，从kafka读的数据:[ConsumerRecord[String(分区), String(jsonSTR)]]===>jsonOBJ
    //因为转换成json对象好操作
    val jsonObjDstream: DStream[JSONObject] = offsetDstream.map(
      mapFunc = record => {
        val jsonSTR: String = record.value()
        val jsonObj: JSONObject = JSON.parseObject(jsonSTR)
        jsonObj
      }
    )

    //进行分流处理，根据不同的表名，发送到不同的kafka主题中
    jsonObjDstream.foreachRDD{
      rdd=>{
        rdd.foreach(
          jsonObj=>{
            val optype: String = jsonObj.getString("type")
            //判断是否是新增
            if("INSERT".equals(optype)){
              //获取表名
              val tbaleName: String = jsonObj.getString("table")
              //可以拿到多值的json数组
              val dataArr: JSONArray = jsonObj.getJSONArray("data")
              //拼接目标topic名称
              var sendTopic="ods_"+tbaleName
              //对dataArr进行遍历
              import scala.collection.JavaConverters._
              for (elem <- dataArr.asScala) {
                //根据表名将数据发送到不同的主题中
                MykafkaSink.send(sendTopic,elem.toString)
              }
            }
          }
        )
        //提交偏移量
        OffsetManagerUtil.saveOffset(topic,groupId,offsetRanges)
      }
    }
    ssc.start()
    ssc.awaitTermination()

  }
}