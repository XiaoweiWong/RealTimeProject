package com.atguigu.gmall.controller;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author david
 * @create 2020-09-08 上午 11:01
 */

//标识为controller组件，交给Sprint容器管理，并接收处理请求  如果返回String，会当作网页进行跳转
// @RestController = @Controller + @ResponseBody  会将返回结果转换为json进行响应

@RestController
@Slf4j
public class LoggerController {
    //通过requestMapping匹配请求并交给方法处理（这里要匹配是applog结尾的请求）,
    // 根据不同的网址请求匹配不同的处理请求方法

    /**
     * 在模拟数据生成的代码中，我们将数据封装为json，
     * 通过post传递给该Controller处理，所以我们通过@RequestBody接收
     */
    @Autowired
    KafkaTemplate kafkaTemplate;
    @RequestMapping("/applog")
    public String applog(@RequestBody String jsonLog){
        //System.out.println(jsonLog);
       // Logger log = org.slf4j.LoggerFactory.getLogger(LoggerController.class);
        //日志落盘
        log.info(jsonLog);
        //将采集的日志分流
        JSONObject jsonObject = JSON.parseObject(jsonLog);
        if (jsonObject.getJSONObject("start") != null){
            //启动日志
            kafkaTemplate.send("gmall_start_bak",jsonLog);
        }else{
            //时间日志
            kafkaTemplate.send("gmall_event_bak",jsonLog);
        }

        return "success";
    }

}
