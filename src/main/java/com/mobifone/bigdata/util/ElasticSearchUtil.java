package com.mobifone.bigdata.util;

import com.mobifone.bigdata.common.AppConfig;
import org.apache.http.HttpHost;


import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public class ElasticSearchUtil {
    private static ElasticSearchUtil instance;
    private RestHighLevelClient clientElastic;
    public static ElasticSearchUtil getInstance() throws IOException {
        if (instance==null){
            return new ElasticSearchUtil();
        }
        return instance;
    }
    public ElasticSearchUtil() throws IOException {
        Properties appProperties = AppConfig.getAppConfigProperties();
        clientElastic= new RestHighLevelClient(
                RestClient.builder(new HttpHost(appProperties.getProperty("elasticsearch.host"),Integer.parseInt(appProperties.getProperty("elasticsearch.port")),"http"))
        );
        ClusterHealthResponse response = clientElastic.cluster().health(new ClusterHealthRequest(), RequestOptions.DEFAULT);
        ActionListener<ClusterHealthResponse> listener = ActionListener.<ClusterHealthResponse>wrap(
                r -> System.out.println(),Throwable::printStackTrace
        );
        clientElastic.cluster().healthAsync(new ClusterHealthRequest(),RequestOptions.DEFAULT,listener);
    }
    public RestHighLevelClient GetClientElasticSearch(){
        return clientElastic;
    }
    public void InsertJson(Map<String, Object> jsonMap, String index){
        IndexRequest request = new IndexRequest(index)
                .source(jsonMap);
        clientElastic.indexAsync(request, RequestOptions.DEFAULT,null);
    }
}
