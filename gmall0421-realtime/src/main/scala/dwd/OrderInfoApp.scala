package dwd

import Bean.{OrderInfo, UserStatus}
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import utils.{MyKafkaUtil, OffsetManagerUtil, PhoenixUtil}

/**
 * @author david 
 * @create 2020-09-18 上午 8:40
 */
object OrderInfoApp {
  def main(args: Array[String]): Unit = {

    //TODO 1.从Kafka中查询订单信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("OrderInfoApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topic = "ods_order_info"
    val groupId = "order_info_group"

    // 从Redis中读取Kafka偏移量
    val kafkaOffsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic,groupId)

    var recordDstream: InputDStream[ConsumerRecord[String, String]] = null

    if(kafkaOffsetMap!=null && kafkaOffsetMap.size>0){
      //获取到Redis中kafka的偏移量  根据Redis中保存的偏移量开始读取数据
      recordDstream = MyKafkaUtil.getKafkaStream(topic, ssc,kafkaOffsetMap,groupId)
    }else{
      // Redis中没有保存偏移量  Kafka默认从最新读取
      recordDstream = MyKafkaUtil.getKafkaStream(topic, ssc,groupId)
    }


    //获取本批次中处理数据的分区对应的偏移量起始及结束位置
                  // 注意：这里我们从Kafka中读取数据之后，直接就获取了偏移量的位置，因为KafkaRDD可以转换为HasOffsetRanges，会自动记录位置
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange] // var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val offsetDStream: DStream[ConsumerRecord[String, String]] = recordDstream.transform {
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }

    //对从Kafka中读取到的数据进行结构转换，由Kafka的ConsumerRecord转换为一个OrderInfo对象
    val orderInfoDStream: DStream[OrderInfo] = offsetDStream.map {
      record => {
        val jsonString: String = record.value()
        val orderInfo: OrderInfo = JSON.parseObject(jsonString,classOf[OrderInfo])
        //通过对创建时间2020-07-13 01:38:16进行拆分，赋值给日期和小时属性，方便后续处理
        val createTimeArr: Array[String] = orderInfo.create_time.split(" ")
        //获取日期赋给日期属性
        orderInfo.create_date = createTimeArr(0)
        //获取小时赋给小时属性
        orderInfo.create_hour = createTimeArr(1).split(":")(0)
        orderInfo
      }
    }
    /*
    //方案1：对DStream中的数据进行处理，判断下单的用户是否为首单
    //缺点：每条订单数据都要执行一次SQL，SQL执行过于频繁
    val orderInfoWithFirstFlagDStream: DStream[OrderInfo] = orderInfoDStream.map {
      orderInfo => {
        //通过phoenix工具到hbase中查询用户状态
        var sql: String = s"select user_id,if_consumed from user_status2020 where user_id ='${orderInfo.user_id}'"
        val userStatusList: List[JSONObject] = PhoenixUtil.queryList(sql)
        if (userStatusList != null && userStatusList.size > 0) {
          orderInfo.if_first_order = "0"
        } else {
          orderInfo.if_first_order = "1"
        }
        orderInfo
      }
    }
    orderInfoWithFirstFlagDStream.print(1000)
    */
    //TODO 方案2：对DStream中的数据进行处理，判断下单的用户是否为首单
    //优化:以分区为单位，将一个分区的查询操作改为一条SQL
    //为订单信息增加首单标识
    val orderInfoWithFirstFlagDStream: DStream[OrderInfo] = orderInfoDStream.mapPartitions {
      orderInfoItr => {
        //迭代器放的是该分区所有订单，迭代器可以理解为一个指针，因为迭代器迭代之后就获取不到数据了，所以将迭代器转换为集合进行操作
        val orderInfoList: List[OrderInfo] = orderInfoItr.toList
        //对订单集合进行转换，获取当前分区内的用户ids
        val userIdList: List[Long] = orderInfoList.map(_.user_id)
        //从hbase中查询整个分区的用户是否消费过，获取消费过的用户ids集合
        var sql: String = s"select user_id,if_consumed from user_status0421 where user_id in('${userIdList.mkString("','")}')"
        //注1：  var sql: String = s"select user_id,if_consumed from user_status2020 where user_id in('${userIdList.mkString("','")}')"
        val userStatusList: List[JSONObject] = PhoenixUtil.queryList(sql)
        //得到已消费过的用户的id集合,    phoenix中默认字段都是大写
        val cosumedUserIdList: List[String] = userStatusList.map(_.getString("USER_ID"))

        //对分区数据进行遍历
        for (orderInfo <- orderInfoList) {
          if (cosumedUserIdList.contains(orderInfo.user_id.toString)) {
            //注2 cosumedUserIdList.contains(orderInfo.user_id.toString)
            //如已消费过的用户的id集合包含当前下订单的用户，说明不是首单，设为0
            orderInfo.if_first_order = "0"
          } else {
            orderInfo.if_first_order = "1"
          }
        }
        orderInfoList.toIterator//将集合转换为迭代器
      }
    }
    orderInfoWithFirstFlagDStream.print()

      //TODO 4. 同批次同用户状态修正
    //因为DStream只能对KV 进行分组，所以先对结构进行转换，使用groupByKey分组
    val orderInfoWithKeyDStream: DStream[(Long, OrderInfo)] = orderInfoWithFirstFlagDStream.map {
      orderInfo => {
        (orderInfo.user_id, orderInfo)
      }
    }
    //按照用户Id对当前采集周期数据进行分组
    val groupByKeyDStream: DStream[(Long, Iterable[OrderInfo])] = orderInfoWithKeyDStream.groupByKey()
    //对分组后的数据进行判断
    groupByKeyDStream.map{
      case(userId,orderInfoItr)=>{
        //如果同一批次有用户的订单个数大于1
        if(orderInfoItr.size>1){
          //对用户订单按照时间的顺序进行排序
          val sortList: List[OrderInfo] = orderInfoItr.toList.sortWith(
            (orderInfo1, orderInfo2) => {
              orderInfo1.create_time < orderInfo2.create_time
            }
          )
          //获取集合排序后的第一个元素
          val orderInfoFirst: OrderInfo = sortList(0)
         if (orderInfoFirst.if_first_order == "1"){ //不用再判断 ==0 的了，第一个元素的if_first_order==0肯定后面都是0了
           for (i <- 1 to sortList.size-1) {
             val orderInfoNotFirst: OrderInfo = sortList(i)
             orderInfoNotFirst.if_first_order = "0"
           }
         }
          sortList
        }else {
          orderInfoItr.toList
        }
      }

    }
    //TODO 5.维度关联（通过phoenix查询）


    //TODO 3.  维护hbase中用户首单状态（将是否消费的数据保存到Hbase的表里）
    //导入类下成员
    import org.apache.phoenix.spark._
    orderInfoWithFirstFlagDStream.foreachRDD(

      rdd =>{
        //从所有订单中过滤首单
        val firstOrderRDD: RDD[OrderInfo] = rdd.filter(_.if_first_order=="1")
        //获取当前订单用户并更新到Hbase，注意：saveToPhoenix在更新的时候，要求rdd中的属性和插入hbase表中的列必须保持一致，所以转换一下
        val firstOrderUserRDD: RDD[UserStatus] = firstOrderRDD.map(
          orderInfo => {
            UserStatus(orderInfo.user_id.toString, "1")
          }
        )
        firstOrderUserRDD.saveToPhoenix(
          "USER_STATUS0421",
          Seq("USER_ID","IF_CONSUMED"),
          new Configuration,
          Some("hadoop102,hadoop103,hadoop104:2181")
        )
        //保存偏移量到Redis
        OffsetManagerUtil.saveOffset(topic,groupId,offsetRanges)

      }
    )
    ssc.start()
    ssc.awaitTermination()

  }

}
