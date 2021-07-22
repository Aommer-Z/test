package com.atguigu.gmall.util

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, ResultSetMetaData}

import com.alibaba.fastjson.JSONObject

import scala.collection.mutable.ListBuffer

/**
 * @program: gmall0610
 * @description: 用于从phoenix中查询数据
 * @author: zyl
 * @create: 2021-06-18 15:55
 **/

/*
    User_id           if_consumerd
      za                  1
      ls                  1
      ww                  1
      期望结果
      {"User_id":"za","if_consumerd":1}
      {"User_id":"ls","if_consumerd":1}
      {"User_id":"ww","if_consumerd":1}
 */
object PhoenixUtil {
  def main(args: Array[String]): Unit = {
    var sql="select * from user_status0618"
    val rs: List[JSONObject] = queryList(sql)
    println(rs)
  }

  def queryList(sql:String):List[JSONObject]={
    //注册驱动
    Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
    val rsList: ListBuffer[JSONObject] = new ListBuffer[ JSONObject]()
    //建立连接
    val conn: Connection =
      DriverManager.getConnection("jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181")
    //创建数据库操作对象
    val ps: PreparedStatement = conn.prepareStatement(sql)
    //执行sql语句
    /*
     za                  1
      ls                  1
      ww                  1
     */
    val rs: ResultSet = ps.executeQuery()
    //拿到元数据，里面包含各种属性，列名，标签等
    val rsMetaData: ResultSetMetaData = rs.getMetaData
    //处理结果集
    while ( rs.next ) {
      val userStatusJsonObj = new JSONObject()
      //                             列数量
      for (i <- 1 to rsMetaData.getColumnCount){
        userStatusJsonObj.put(rsMetaData.getColumnName(i),rs.getObject(i))

      }
      rsList.append(userStatusJsonObj)
    }
    //释放资源
    rs.close()
    ps.close()
    conn.close()
    rsList.toList
  }
}