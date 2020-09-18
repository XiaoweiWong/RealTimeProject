package utils

import java.sql.{Connection, DriverManager, ResultSet, ResultSetMetaData, Statement}

import com.alibaba.fastjson.JSONObject

import scala.collection.mutable.ListBuffer

/**
 * 查询phoenix的工具类
 *
 * @author david 
 * @create 2020-09-17 下午 3:01 
 */
object PhoenixUtil {

  def main(args: Array[String]): Unit = {

  }

  def queryList(sql:String)={
    //注册驱动
    Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
    val resultList: ListBuffer[JSONObject] = new ListBuffer[JSONObject]()
    //创建连接
    val connection: Connection = DriverManager.getConnection("jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181")
    val stat: Statement = connection.createStatement()
    //println(sql)
    //执行查询SQL,得到查询结果
    val resultSet: ResultSet = stat.executeQuery(sql)
    //
    val metaData: ResultSetMetaData = resultSet.getMetaData//?
    while(resultSet.next()){
      val rowData: JSONObject = new JSONObject()
      for(i <- 1 to metaData.getColumnCount){
        rowData.put(metaData.getColumnName(i),resultSet.getObject(i))
      }
      resultList+=rowData
    }


    stat.close()
    connection.close()
    resultList.toList
  }
}
