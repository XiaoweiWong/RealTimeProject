package utils
/**
 * 获取Redis连接的工具类
 * @author david 
 * @create 2020-09-12 上午 11:46 
 */

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

  object MyRedisUtil {
    var jedisPool:JedisPool=null
   // val jedisclieny:JedisPool=_
    /**
     * 获取jedis客户端方法
     * @return
     */
    def getJedisClient: Jedis = {
      if(jedisPool==null){
        val config = MyPropertiesUtil.load("config.properties")
        val host: String = config.getProperty("redis.host")
        val port: String = config.getProperty("redis.port")

        val jedisPoolConfig = new JedisPoolConfig()
        jedisPoolConfig.setMaxTotal(100)  //最大连接数
        jedisPoolConfig.setMaxIdle(20)   //最大空闲
        jedisPoolConfig.setMinIdle(20)     //最小空闲
        jedisPoolConfig.setBlockWhenExhausted(true)  //忙碌时是否等待
        jedisPoolConfig.setMaxWaitMillis(5000)//忙碌时等待时长 毫秒
        jedisPoolConfig.setTestOnBorrow(true) //每次获得连接的进行测试

        jedisPool=new JedisPool(jedisPoolConfig,host,port.toInt)
      }
      //拿到一个Jedis对象
      jedisPool.getResource
    }

    def main(args: Array[String]): Unit = {
      val jedisClient = getJedisClient
      println(jedisClient.ping())
      //关闭连接
      jedisClient.close()
    }
  }


