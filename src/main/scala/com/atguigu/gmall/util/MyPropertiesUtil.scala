package com.atguigu.gmall.util

import java.io.{FileInputStream, InputStreamReader}
import java.nio.charset.StandardCharsets
import java.util.Properties

/**
 * @program: sparkGmall
 * @description: ${description}
 * @author: zyl
 * @create: 2021-06-06 21:43
 **/
object MyPropertiesUtil {
  def main(args: Array[String]): Unit = {
    val properties: Properties = MyPropertiesUtil.load("config.properties")
    println(properties.getProperty("kafka.broker.list"))

  }

  //propertiesName:配置文件路径  Properties:封装个对象返回
  def load(propertiesName:String):Properties={
    val prop = new Properties()
    //加载指定的配置文件  //路径不能写绝对路径，换了环境就可能不会再同一路径下了，所以从打完包的target下class下拿
    prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertiesName),StandardCharsets.UTF_8))
    prop
  }

}