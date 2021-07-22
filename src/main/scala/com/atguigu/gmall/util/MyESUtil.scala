package com.atguigu.gmall.util

import com.atguigu.gmall.bean.DauInfo
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, BulkResult, Index}

/**
 * @program: gmall0610
 * @description: ${description}
 * @author: zyl
 * @create: 2021-06-12 17:57
 **/
object MyESUtil {
  //像ES中批量插入数据
  def bulkInsert(dauInFoLiST: List[DauInfo], indexName: String): Unit ={

    if (dauInFoLiST!=null && dauInFoLiST.size!=0){
      //获取客户端
      val jestClient: JestClient = getJestClient()
      val bulkBuilder = new Bulk.Builder()
      for (dauInfo <- dauInFoLiST) {
        val index: Index = new Index.Builder(dauInfo)
          .index(indexName)
          .`type`("_doc")
          .build()
        bulkBuilder.addAction(index)
      }

      //创建批量操作对象
      val bulk: Bulk = bulkBuilder.build()
      val bulkResult: BulkResult = jestClient.execute(bulk)
      println("向ES中插入"+bulkResult.getItems().size()+"条数据")
      jestClient.close()
    }


  }

  //拿到Jest客户端
  //声明Jest客户端工厂
  private var jestFactory:JestClientFactory=null

  def build() = {
    jestFactory=new JestClientFactory
    //builder:构造者设计模式
    jestFactory.setHttpClientConfig(new HttpClientConfig
    .Builder("http://hadoop102:9200")
      .multiThreaded(true)
      .maxTotalConnection(20)
      .connTimeout(10000)
      .readTimeout(1000).build())
  }

  //提供获取Jest客户端的方法
  def getJestClient():JestClient= {
    if (jestFactory == null){
      //创建Jest客户端工厂对象
      //专门封装一个方法去创建工厂对象
      build()
    }
    jestFactory.getObject

  }
}