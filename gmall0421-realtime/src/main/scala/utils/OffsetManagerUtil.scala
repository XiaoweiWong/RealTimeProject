package utils

import java.util
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis

/**
 *偏移量管理类，用于读取和保存偏移量
 * @author david 
 * @create 2020-09-14 上午 11:39 
 */
object OffsetManagerUtil {
  /**
   *从redis中读取偏移量
   * @param topicName
   * @param groupId
   * @return
   */

  /**
   *  从Redis中读取偏移量
   *  Reids格式：type=>Hash  [key=>offset:topic:groupId field=>partitionId value=>偏移量值] expire 不需要指定
   * @param topicName  主题名称
   * @param groupId  消费者组
   * @return 当前消费者组中，消费的主题对应的分区的偏移量信息
   *         KafkaUtils.createDirectStream在读取数据的时候封装了Map[TopicPartition,Long]
   */

  def getOffset(topicName:String,groupId:String):Map[TopicPartition,Long]={

    val client: Jedis = MyRedisUtil.getJedisClient
                   //拼接Reids中存储偏移量的key
    var offsetKey = "offset:"+topicName+":"+groupId
                 //根据key从Reids中获取数据
    val offsetMap: util.Map[String, String] = client.hgetAll(offsetKey)

    client.close()
    import scala.collection.JavaConverters._

    val kafkaOffsetMap: Map[TopicPartition, Long] = offsetMap.asScala.map {
                //有case的时候，传递单一参数，map（）必须要改为大括号，如果当前函数的有两个及以上参数，必须是小括号
      case (partitionId, offset) => {
        println("读取分区偏移量：" + partitionId + ":" + offset)
        //将Redis中保存的分区对应的偏移量进行封装
        (new TopicPartition(topicName, partitionId.toInt), offset.toLong)
      }
    }.toMap  //将可变的Map集合转换为不可变
    kafkaOffsetMap
  }


/**
 * 向Redis中保存偏移量
 * Reids格式：type=>Hash  [key=>offset:topic:groupId field=>partitionId value=>偏移量值] expire 不需要指定
*/
  def saveOffset(topicName:String,groupId:String,offsetRanges:Array[OffsetRange]): Unit ={
    //定义JAVA的map集合，用于向redis中保存数据
    val offsetMap: util.HashMap[String, String] = new util.HashMap[String,String]()
    //对封装的偏移量数组offsetRanges进行遍历
    for (offset <- offsetRanges) {
      //获取分区
      val partition: Int = offset.partition
      //获取结束点
      val untilOffset: Long = offset.untilOffset
      //封装到map集合
      offsetMap.put(partition.toString,untilOffset.toString)

      println("保存分区:" + partition +"偏移量的起始到终止:" + offset.fromOffset+"--->" + offset.untilOffset)
    }
    //拼接Reids中存储偏移量的key
    val offsetKey :String = "offset:"+topicName+":"+groupId
    //如果保存的偏移量不为空
    if(offsetMap != null && offsetMap.size()>0){
      val jedis:Jedis = MyRedisUtil.getJedisClient

      jedis.hmset(offsetKey,offsetMap)

      jedis.close()
    }
  }

}
