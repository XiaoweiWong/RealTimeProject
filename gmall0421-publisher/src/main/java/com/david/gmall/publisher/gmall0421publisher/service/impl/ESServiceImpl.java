package com.david.gmall.publisher.gmall0421publisher.service.impl;

import com.david.gmall.publisher.gmall0421publisher.service.ESService;

import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author david
 * @create 2020-09-15 上午 10:04
 */
@Service
public class ESServiceImpl implements ESService {
    @Autowired
    JestClient jestClient;

    /**
     * 获取天的日活数
     * @param date
     * @return
     */
    @Override
    public Long getDauTotal(String date) {

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(new MatchAllQueryBuilder());
        String query = searchSourceBuilder.toString();

        //拼接查询索引的名称
        String indexName = "gmall2020_dau_info_" + date + "-query";
        Search search = new Search.Builder(query).addIndex(indexName).addType("_doc").build();
        Long total = 0L;
        try{
            SearchResult searchResult = jestClient.execute(search);
            if (searchResult.getTotal() != null){
                total = searchResult.getTotal();
                }
            }catch (IOException e){
            e.printStackTrace();
                throw new RuntimeException("ES查询异常");
            }
        return total;

    }

    /*
    查询语句，查询分时值,对语句进行封装
   GET /gmall0421_dau_info_2020-09-15-query/_search
   {
        "aggs": {
        "groupby_hr": {
            "terms": {
                "field": "hr",
                        "size": 24
            }
        }
    }
    }*/

    @Override
    public Map<String, Long> getDauHour(String date) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //以hr字段分组聚合，指定字段和大小，
        TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders.terms("groupby_hr")
                .field("hr").size(24);
        searchSourceBuilder.aggregation(termsAggregationBuilder);
        String query = searchSourceBuilder.toString();
        //  拼接查询的索引名称
        String indexName = "gmall2020_dau_info_" + date + "-query";
        Search search = new Search.Builder(query).addIndex(indexName).addType("_doc").build();
        try {
            Map<String,Long> dauMap = new HashMap<>();
            SearchResult searchResult = jestClient.execute(search);
            if (searchResult.getAggregations().getTermsAggregation("groupby_hr") != null){
                List<TermsAggregation.Entry> buckets = searchResult.getAggregations()
                        .getTermsAggregation("groupby_hr").getBuckets();//?????
                for (TermsAggregation.Entry bucket : buckets) {
                    dauMap.put(bucket.getKey(),bucket.getCount());  //????
                }
            }
            return dauMap;
        }catch (IOException e){
            e.printStackTrace();
            throw new RuntimeException("ES查询异常");
        }
    }
}
