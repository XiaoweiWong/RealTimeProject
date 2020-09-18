package ods

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import utils.{MyKafkaSink, MyKafkaUtil, OffsetManagerUtil}

/**
 * @author david 
 * @create 2020-09-16 下午 1:33 
 */
object BaseDBCanalApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("BaseDBCanalApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topic = "gmall0421_db_c"
    val groupId = "base_db_canal_group"

    //从Redis中读取Kafka偏移量
    val kafkaOffsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic,groupId)

    var recordDstream: InputDStream[ConsumerRecord[String, String]] = null

    if(kafkaOffsetMap!=null&&kafkaOffsetMap.size>0){
      //Redis中有偏移量  根据Redis中保存的偏移量读取
      recordDstream = MyKafkaUtil.getKafkaStream(topic, ssc,kafkaOffsetMap,groupId)
    }else{
      // Redis中没有保存偏移量  Kafka默认从最新读取
      recordDstream = MyKafkaUtil.getKafkaStream(topic, ssc,groupId)
    }

    //得到本批次中处理数据的分区对应的偏移量起始及结束位置
    // 注意：这里我们从Kafka中读取数据之后，直接就获取了偏移量的位置，因为KafkaRDD可以转换为HasOffsetRanges，会自动记录位置
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val offsetDStream: DStream[ConsumerRecord[String, String]] = recordDstream.transform {
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        println(offsetRanges(0).untilOffset + "*****")
        rdd
      }
    }

    //对从Kafka中读取到的数据进行结构转换，由Kafka的ConsumerRecord转换为一个Json对象
    val jsonObjDStream: DStream[JSONObject] = offsetDStream.map {
      record => {
        val jsonString: String = record.value()
        val jsonObj: JSONObject = JSON.parseObject(jsonString)
        jsonObj
      }
    }
    //从json对象中获取data数组以及table属性值
    jsonObjDStream.foreachRDD{
      rdd=>{
        //对读取到的RDD进行遍历
        rdd.foreach(jsonObj=>{
          //通过jsonObj获取data数据
          val dataArr: JSONArray = jsonObj.getJSONArray("data")
          //通过jsonObj获取table名称
          val tableName: String = jsonObj.getString("table")
          //拼接发送的Topic
          var sendTopic = "ods_"+tableName
          //将dataArr转换为Scala数组，并对dataArr数组进行遍历
          import scala.collection.JavaConverters._
          for(data<-dataArr.asScala){
            val msg: String = data.toString
            //向Kafka发送数据
            MyKafkaSink.send(sendTopic,msg)
          }
        })
        OffsetManagerUtil.saveOffset(topic,groupId,offsetRanges)
      }
    }

    ssc.start()
    ssc.awaitTermination()

  }


}
