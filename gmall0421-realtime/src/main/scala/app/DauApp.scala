package app
import java.lang
import java.text.SimpleDateFormat
import java.util.Date
import Bean.DauInfo
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkConf
import redis.clients.jedis.Jedis
import utils.{MyESUtil, MyKafkaUtil, MyRedisUtil, OffsetManagerUtil}

import scala.collection.mutable.ListBuffer

/**
 * @author david 
 * @create 2020-09-12 下午 1:53 
 */
object DauApp {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("DauApp").setMaster("local[*]")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))
    val groupId = "gmall_dau_bak"
    val topic = "gmall_start_bak"
    //TODO 功能1  sparkstream消费kafka数据

    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)
    var recordDstream:InputDStream[ConsumerRecord[String,String]] = null
    if (offsetMap != null && offsetMap.size > 0) {
      //Redis中有偏移量  根据Redis中保存的偏移量读取
      recordDstream = MyKafkaUtil.getKafkaStream(topic, ssc,offsetMap,groupId)
    } else {
      // Redis中没有保存偏移量  Kafka默认从最新读取
      recordDstream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
  }
    //从读取到的kafka数据，ds流转换为带有偏移量的特质类型，并获取到offset（转换）
    var offsetRanges: Array[OffsetRange]= Array.empty[OffsetRange]
    val offsetDStream: DStream[ConsumerRecord[String, String]] = recordDstream.transform {
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        println(offsetRanges(0).untilOffset + "*****")
        rdd
      }
    }




    //测试输出1
    //recordDstream.map(_.value()).print(100)
    val jsonObjDStream: DStream[JSONObject] = offsetDStream.map { record => {
      //获取启动日志
      val jsonStr: String = record.value()
      //将启动日志转换为json对象
      val jsonObj: JSONObject = JSON.parseObject(jsonStr)
      //获取时间戳 毫秒数
      val ts: lang.Long = jsonObj.getLong("ts")
      //获取字符串   日期 小时
      val dateHourString: String = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(ts))
      //对字符串日期和小时进行分割，分割后放到json对象中，方便后续处理
      val dateHour: Array[String] = dateHourString.split(" ")
      jsonObj.put("dt", dateHour(0))
      jsonObj.put("hr", dateHour(1))
      jsonObj
    }
    }
    //测试输出2
    // jsonObjDStream.print()



    //TODO 功能2 使用redis过滤日活数据（"start"出现一次为记录，再出现就去重，采用redis的set存储结构）
    //方法1.
    //方法2.以分区为单位进行过滤，可以减少和连接池交互的次数
    val filteredDStream: DStream[JSONObject] = jsonObjDStream.mapPartitions {
      jsonObjItr => {
        //获取Redis客户端
        val jedisClient: Jedis = MyRedisUtil.getJedisClient
        //定义当前分区过滤后的数据
        val filteredList: ListBuffer[JSONObject] = new ListBuffer[JSONObject]

        for (jsonObj <- jsonObjItr) {
          //获取当前日期
          val dt: String = jsonObj.getString("dt")
          //获取设备mid
          val mid: String = jsonObj.getJSONObject("common").getString("mid")
          //拼接向Redis放的数据的key
          val dauKey: String = "dau:" + dt
          //判断Redis中是否存在该数据
          val isNew: lang.Long = jedisClient.sadd(dauKey,mid)//添加数据到redis的set结构中
          //设置redis中当天的key数据失效时间为24小时
          jedisClient.expire(dauKey,3600*24)

          if (isNew == 1L) {
            //如果Redis中不存在，那么将数据添加到新建的ListBuffer集合中，实现过滤的效果
            filteredList.append(jsonObj)
          }
        }
        jedisClient.close()
        filteredList.toIterator
      }
    }
    //输出测试    数量会越来越少，最后变为0   因为我们mid只是模拟了50个
   // filteredDStream.count().print()

    //TODO 功能3 向ES中保存日活数据
    filteredDStream.foreachRDD{ //行动算子foreachRDD
      rdd=>{//获取DS中的RDD（RDD本质是集合，保存的是计算逻辑，封装的大量的json对象数据）
        rdd.foreachPartition{//以分区为单位对RDD中的json对象数据进行批量处理，jsonIter,保存到ES
          jsonItr=>{
            //将每个分区的数据转换为List方便进行处理
            val dauList: List[(String,DauInfo)] = jsonItr.map {
              jsonObj => {
                //每次处理的是一个json对象   将json对象封装为样例类
                val commonJsonObj: JSONObject = jsonObj.getJSONObject("common")
                val dauInfo: DauInfo = DauInfo(
                  commonJsonObj.getString("mid"),//设备id
                  commonJsonObj.getString("uid"),//用户ID
                  commonJsonObj.getString("ar"),//地区
                  commonJsonObj.getString("ch"),//渠道
                  commonJsonObj.getString("vc"),//版本
                  jsonObj.getString("dt"),//天
                  jsonObj.getString("hr"),//小时
                  "00", //分钟我们前面没有转换，默认00
                  jsonObj.getLong("ts") //时间戳
                )
                (dauInfo.mid,dauInfo)
              }
            }.toList
            //对分区的数据进行批量处理
            //获取当前日志日期字符串
            val dt: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
            MyESUtil.bulkInsert(dauList,"gmall2020_dau_info_" + dt)
          }
        }
        //保存提交偏移量
        OffsetManagerUtil.saveOffset(topic,groupId,offsetRanges)
      }
    }

    //启动采集
    ssc.start()
    ssc.awaitTermination()

  }

}
