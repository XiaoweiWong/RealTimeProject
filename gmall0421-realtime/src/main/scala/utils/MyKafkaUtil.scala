package utils
import java.util.Properties
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

/**
 * @author david 
 * @create 2020-09-12 上午 11:15 
 */
object MyKafkaUtil {

  private val properties: Properties = MyPropertiesUtil.load("config.properties")
  val broker_list = properties.getProperty("kafka.broker.list")//list可自定义

  // kafka消费者配置
  var kafkaParam = collection.mutable.Map(
    "bootstrap.servers" -> broker_list,//用于初始化链接到集群的地址
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    //用于标识这个消费者属于哪个消费团体
    "group.id" -> "gmall2020_group",
    //latest自动重置偏移量为最新的偏移量
    "auto.offset.reset" -> "latest",
    //如果是true，则这个消费者的偏移量会在后台自动提交,但是kafka宕机容易丢失数据
    //如果是false，会需要手动维护kafka偏移量
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )


  /**
   * kafka配置数据，封装成DStream，返回接收到的输入数据
   * @param topic
   * @param ssc
   * @return
   */
  def getKafkaStream(topic: String,ssc:StreamingContext ): InputDStream[ConsumerRecord[String,String]]={
    val dStream = KafkaUtils.createDirectStream[String,String](
      //上下文变量
      ssc,
      //位置策略
      LocationStrategies.PreferConsistent,
      //消费策略
      ConsumerStrategies.Subscribe[String,String](Array(topic), kafkaParam )
    )
    dStream
  }

  /**
   *
   * @param topic
   * @param ssc
   * @param groupId
   * @return
   */
  def getKafkaStream(topic: String,ssc:StreamingContext,groupId:String): InputDStream[ConsumerRecord[String,String]]={
   //指定groupId
    kafkaParam("group.id")=groupId
    val dStream = KafkaUtils.createDirectStream[String,String](
      //上下文变量
      ssc,
      //位置策略
      LocationStrategies.PreferConsistent,
      //消费策略
      ConsumerStrategies.Subscribe[String,String](Array(topic),kafkaParam ))
    dStream
  }

  /**
   *
   * @param topic
   * @param ssc
   * @param offsets
   * @param groupId
   * @return
   */
  def getKafkaStream(topic: String,ssc:StreamingContext,offsets:Map[TopicPartition,Long],groupId:String)
  : InputDStream[ConsumerRecord[String,String]]={
    //指定groupId
    kafkaParam("group.id")=groupId

    val dStream = KafkaUtils.createDirectStream[String,String](
      ssc,//上下文变量
      LocationStrategies.PreferConsistent,//位置策略
      //消费策略，[String,String]是kafka数据K,V类型，
      ConsumerStrategies.Subscribe[String,String](Array(topic),kafkaParam,offsets))

    dStream
  }

}
