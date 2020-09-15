package com.david.gmall.publisher.gmall0421publisher.controller;

import com.david.gmall.publisher.gmall0421publisher.service.ESService;
import org.apache.commons.lang3.time.DateUtils;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.SimpleDateFormat;
import java.util.*;


/**
 * @author david
 * @create 2020-09-15 上午 10:04
 */
@RestController
public class PublisherController {
    @Autowired
    ESService esService;
    //访问路径：http://publisher:8070/realtime-total?date=2019-02-01
    /*
    //返回数据格式
    [
        {
            "id":"dau",
            "name":"新增日活",
            "value":1200
        },
    {"id":"new_mid","name":"新增设备","value":233}
    ]
    */
    @RequestMapping("/realtime-total")
    public Object realtimeTotal(@RequestParam("date") String dt){
        List<Map<String,Object>> dauList = new ArrayList<>();
        //定义一个map集合，用于封装新增日活数据
        Map<String,Object> dauMap = new HashMap<>();
        dauMap.put("id","dau");
        dauMap.put("name","新增日活");
        Long dauTotal = esService.getDauTotal(dt);

        dauMap.put("value",dauTotal);
        dauList.add(dauMap);


        //定义一个map集合，用于封装新增设备数据
        Map<String,Object> midMap = new HashMap<>();
        midMap.put("id","mid");
        midMap.put("name","新增设备");
        midMap.put("value",233);
        dauList.add(midMap);
        return dauList;
    }
/*//发布日活分时值接口
    数据格式：
        {
            "yesterday":{"11":383,"12":123,"17":88,"19":200 },
            "today":{"12":38,"13":1233,"17":123,"19":688 }
        }
    访问路径：http://publisher:8070/realtime-hour?id=dau&date=2019-02-01
    */
@RequestMapping("/realtime-hour")
public Object realTimeHour(@RequestParam("id") String id,@RequestParam("date") String dt){
    if("dau".equals(id)){
        Map<String,Map<String,Long>> hourMap = new HashMap<>();
        //获取今天的日活分时值
        Map<String, Long> dauHourMap = esService.getDauHour(dt);
        hourMap.put("today",dauHourMap);
        //获取昨天的日期字符串
        String yd = yetYd(dt);
        //获取昨天的日活分时值
        Map<String,Long> ydmap =esService.getDauHour(yd);
        hourMap.put("yesterday",ydmap);
        return hourMap;
    }
    return null;
}

    private String yetYd(String td) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String yd = null;
        try {
            Date tdDate = dateFormat.parse(td);
            Date ydDate = DateUtils.addDays(tdDate, -1);
            yd = dateFormat.format(ydDate);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("日期格式转变失败");
        }
            return yd;
    }

}
