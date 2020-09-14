package utils
import java.io.InputStreamReader
import java.util.Properties

/**
 * 读配置文件工具类
 * @author david 
 * @create 2020-09-12 上午 10:59 
 */
object MyPropertiesUtil {
  def main(args: Array[String]): Unit = {
    val properties: Properties = MyPropertiesUtil.load("config.properties")
    println(properties.getProperty("kafka.broker.list"))
  }

  /**
   * 读配置文件方法
   * @param propertieName
   * @return 返回配置对象
   */
  def load(propertieName:String):Properties={
    val prop: Properties = new Properties()
    prop.load(new InputStreamReader(Thread.currentThread()
      .getContextClassLoader.getResourceAsStream(propertieName),"UTF-8"))
    prop
  }
}
