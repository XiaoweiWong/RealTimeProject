package utils

import java.util

import org.apache.kafka.common.TopicPartition
import redis.clients.jedis.Jedis

/**
 *
 * @author david 
 * @create 2020-09-14 上午 11:39 
 */
object OffsetManagerUtil {

  //TODO
  def getOffset(topicName:String,groupId:String):Map[TopicPartition,Long]={
    val client: Jedis = MyRedisUtil.getJedisClient

    var offsetKey = "offset:"+topicName+":"+groupId
    val offsetMap: util.Map[String, String] = client.hgetAll(offsetKey)
    import scala.collection.JavaConverters._

    offsetMap.asScala.map{//有case的时候（）必须要改为大括号
      case (partitionId,offset)=>{
        (new TopicPartition(topicName,groupId.toInt),offset)

      }

    }

    client.close()
    null
  }

  //TODO

}
