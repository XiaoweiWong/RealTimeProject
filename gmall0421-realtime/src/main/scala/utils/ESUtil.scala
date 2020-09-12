package utils
import java.util
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.core.{DocumentResult, Get, Index, Search, SearchResult}

/**
 * @author david
 * @create 2020-09-11 下午 7:13
 */
object ESUtil {
  var factory:JestClientFactory=_

  def getClient():JestClient={
    if (factory == null){
      build()
    }
    factory.getObject
  }

  def build()={
    factory = new JestClientFactory
    factory.setHttpClientConfig(new HttpClientConfig.Builder("http://hadoop102:9200")
      .multiThreaded(true)
      .maxTotalConnection(20)
      .connTimeout(10000)
      .readTimeout(1000)
      .build())
  }

  def main(args: Array[String]): Unit = {
    //getIndex1()
    //getIndex2()
    //quryIndex1()
      quryIndex2()
  }


  def getIndex1(){
    val client = getClient()
    val source =
      """
        |{
        |    "id":2,
        |    "name":"集结号",
        |    "doubanScore":8.0,
        |    "actorList":[
        |    {"id":3,"name":"张涵予"}
        |    ]
        |  }
      """.stripMargin
    val index: Index = new Index.Builder(source).index("movie_index4").`type`("movie").id("1").build()
    client.execute(index)
    client.close()
  }
  def getIndex2(): Unit ={
      val client = getClient()
      val actorMap1 =new util.HashMap[String,Any]()
      val actorMap2 = new util.HashMap[String,Any]()
      val actorList = new util.ArrayList[util.HashMap[String,Any]]()

    actorMap1.put("id",1)
    actorMap1.put("name","何杰")
    actorMap2.put("id",2)
    actorMap2.put("name","张娜")

    actorList.add(actorMap1)
    actorList.add(actorMap2)

    val index: Index = new Index.Builder(Movie1(12, "泰坦尼克", 9.2, actorList)).index("movie_index4").`type`("movie")
      .id("2").build()
    client.execute(index)
    client.close()

  }
  def quryIndex1(): Unit ={
    var client = getClient()
    val get: Get = new Get.Builder("movie_index4","2").build()
    println(client.execute(get).getJsonString)
    client.close()
  }
  def quryIndex2(): Unit ={
    var client = getClient()
    var queryStr =
      """
        |{
        |  "query": {
        |    "bool": {
        |       "must": [
        |        {"match": {
        |          "name": "红海"
        |        }}
        |      ],
        |      "filter": [
        |        {"term": { "actorList.name.keyword": "张涵予"}}
        |      ]
        |    }
        |  },
        |  "from": 0,
        |  "size": 20,
        |  "sort": [
        |    {
        |      "doubanScore": {
        |        "order": "desc"
        |      }
        |    }
        |  ],
        |  "highlight": {
        |    "fields": {
        |      "name": {}
        |    }
        |  }
        |}
      """.stripMargin
    val search: Search = new Search.Builder(queryStr).addIndex("movie_chn_1").build()
    val result: SearchResult = client.execute(search)
    val list = result.getHits(classOf[util.Map[String,Any]])
    import scala.collection.JavaConverters._
    val list1: List[util.Map[String, Any]] = list.asScala.map(map => map.source).toList
    //如果返回值多条，用换行符隔开
    println(list1.mkString("\n"))
    client.close()
  }
}
case class Movie1(
                id:Long,//电影id
                name:String,
                doubanSorce:Double,
                actorList:java.util.List[java.util.HashMap[String,Any]]
                )
