package utils
import java.util
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{DocumentResult, Get, Index, Search, SearchResult}

/**
 * @author david 
 * @create 2020-09-11 下午 2:47
 *        操作ES的工具类
 */
object MyESUtil {
  private var factory:JestClientFactory = _

  def getClient():JestClient={
  if (factory==null){
    build()
  }
    factory.getObject
  }

  def build()={
    factory = new JestClientFactory
    factory.setHttpClientConfig(
      new HttpClientConfig.Builder("http://hadoop102:9200")
        .multiThreaded(true)
        .maxTotalConnection(20)
        .connTimeout(10000)
        .readTimeout(1000).build()
    )
  }

  /**
   * 在ES里DSL内嵌式的方式增加索引以及文档
   */
  def putIndex1(): Unit ={
    //获取客户端连接
    val client: JestClient = getClient()
    var source=
      """
        |{
        |    "id":2,
        |    "name":"湄公河行动",
        |    "doubanScore":8.0,
        |    "actorList":[
        |    {"id":3,"name":"张涵予"}
        |    ]
        |  }
      """.stripMargin
    val index1 = new Index.Builder(source)
      .index("movie_index3")
      .`type`("movie")
      .id("1").build()
    //执行动作
    client.execute(index1)
    //关闭客户端
    client.close()
  }

  /**
   * 使用样例类的方式向ES的索引增加文档
   */
  def putIndex2(): Unit ={
    //获取连接
    val client = getClient()
    val actorlists = new util.ArrayList[util.HashMap[String,Any]]()

    val actormaps1 = new util.HashMap[String,Any]()
    val actormaps2 = new util.HashMap[String,Any]()
    actormaps1.put("id",10)
    actormaps1.put("name","张明星")
    actormaps2.put("id",12)
    actormaps2.put("name","王明星")

    actorlists.add(actormaps1)
    actorlists.add(actormaps2)
    //
    val index: Index = new Index.Builder(
      Movie(100, "天龙八部", 8.5, actorlists)
    ).index("movie_index3").`type`("movie").id("2").build()
    client.execute(index)
    //关闭连接
    client.close()
  }
//根据文档的id查询ES的索引的数据
  def getIndex1(): Unit ={

    val client = getClient()
    val get: Get = new Get.Builder("movie_index3","1").build()
    val result: DocumentResult = client.execute(get)
    println(result.getJsonString)
    client.close()

  }
  def getIndex2(): Unit ={
    val client = getClient()
    val queryStr=
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
    val list= result.getHits(classOf[util.Map[String,Any]])
    import scala.collection.JavaConverters._
                                                       //把java数据类型的集合转换为scala集合类型,方便操作找到source map键值对
    val resultList = list.asScala.map(_.source).toList
    println(resultList.mkString("\n"))
    client.close()
  }

  /**
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    //putIndex1()
    //putIndex2()
    //getIndex1()
      getIndex2()
  }
}

case class Movie(
                id:Long,
                movie_name:String,
                doubanScore:Double,
                actorlist:java.util.ArrayList[java.util.HashMap[String,Any]]
                ){}