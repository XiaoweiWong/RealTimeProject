package com.david.gmall.publisher.gmall0421publisher.service;

import java.util.Map;

/**
 * @author david
 * @create 2020-09-15 上午 10:05
 */
public interface ESService {
    //获取日活总数
    public Long getDauTotal(String date);
    //日活的分时查询{"00"->10,"01"->20,..."23"->30}
    public Map<String,Long> getDauHour(String date);


}
