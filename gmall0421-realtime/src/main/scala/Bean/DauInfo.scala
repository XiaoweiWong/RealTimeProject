package Bean

/**
 * @author david 
 * @create 2020-09-12 下午 3:35 
 */
case class DauInfo(
                    mid:String,//设备id
                    uid:String,//用户id
                    ar:String,//地区
                    ch:String,//渠道
                    vc:String,//版本
                    var dt:String,//日期
                    var hr:String,//小时
                    var mi:String,//分钟
                    ts:Long //时间戳

                  ){}
