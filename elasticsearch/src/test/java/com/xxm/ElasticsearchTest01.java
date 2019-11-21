package com.xxm;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

/**
 * @Program: IntelliJ IDEA elasticsearch
 * @Description: TODO
 * @Author: Mr Liu
 * @Creed: Talk is cheap,show me the code
 * @CreateDate: 2019-11-17 20:31:59 周日
 * @LastModifyDate:
 * @LastModifyBy:
 * @Version: V1.0
 */
public class ElasticsearchTest01 {
    private TransportClient transportClient;
    //创建 索引(库) 创建 类型(表)，创建 文档(行) -->json
    //每个文档都需要有一个唯一标识
    @Test
    public void add() throws Exception{
        //1.创建客户端对象 连接服务器
        Settings settings= Settings.EMPTY;//(单节点的默认配置)
        TransportClient transportClient=new PreBuiltTransportClient(settings);
        //2.添加服务器的地址port:指定tcp连接端口9300
        transportClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9300));
        //3.创建索引和类型和文档
        //参数一，指定索引的名称
        //参数二，指定类型的名称
        //参数三，指定文档的唯一标识
        Map<String,String> documentMap=new HashMap<String,String>();
        documentMap.put("id","1");
        documentMap.put("name","zhangsan");
        documentMap.put("title","学习elasticsearch");
        documentMap.put("content","elasticsearch是一个非常著名的搜索服务器");
        IndexResponse indexResponse = transportClient.prepareIndex("blog001", "article", "1").setSource(documentMap).get();
        System.out.println("获取到所有的索引名："+indexResponse.getIndex());
        System.out.println("执行的版本："+indexResponse.getVersion());
        System.out.println("类型："+indexResponse.getType());
        //4.关闭客户端对象
        transportClient.close();
    }
    @Test
    public void addByJson() throws Exception{
        //1.创建客户端对象 连接服务器
        Settings settings= Settings.EMPTY;//(单节点的默认配置)
        transportClient=new PreBuiltTransportClient(settings);
        //2.添加服务器的地址port:指定tcp连接端口9300
        transportClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9300));
        //3.创建索引和类型和文档
        //参数一，指定索引的名称
        //参数二，指定类型的名称
        //参数三，指定文档的唯一标识

        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
                .startObject()
                .field("id","3")
                .field("name","zhangsan")
                .field("title","学习elasticsearch")
                .field("content","elasticsearch是一个非常著名的搜索服务器")
                .endObject();
        IndexResponse indexResponse = transportClient.prepareIndex("blog001", "article", "2").setSource(xContentBuilder).get();
        System.out.println("获取到所有的索引名："+indexResponse.getIndex());
        System.out.println("执行的版本："+indexResponse.getVersion());
        System.out.println("类型："+indexResponse.getType());
        //4.关闭客户端对象
        transportClient.close();
    }
    @Before
    public void setClient() throws Exception{
        //1.创建客户端对象 连接服务器
        Settings settings= Settings.EMPTY;//(单节点的默认配置)
        transportClient=new PreBuiltTransportClient(settings);
        //2.添加服务器的地址port:指定tcp连接端口9300
        transportClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9300));
    }
    @After
    public void close(){
        //6.关闭客户端对象，释放资源
        transportClient.close();
    }
    //查询所有数据
    @Test
    public void selectMatchAllQuery(){
        //3.创建查询对象，设置查询条件，并执行查询
        SearchResponse searchResponse = transportClient
                .prepareSearch("blog001")
                .setTypes("article")
                .setQuery(QueryBuilders.matchAllQuery())
                .get();
        //4.获取结果
        SearchHits hits = searchResponse.getHits();
        System.out.println("查询到的命中数总数量"+hits.getTotalHits());
        //5.遍历结果集
        for (SearchHit hit : hits) {//hit就是一个一个的文档
            String sourceAsString = hit.getSourceAsString();
            System.out.println(sourceAsString);
            Map<String, Object> source = hit.getSource();
            System.out.println("打印"+source);
        }
    }
    @Test
    public void selectQueryString(){
        SearchResponse searchResponse = transportClient
                .prepareSearch("blog001")
                .setTypes("article")
                .setQuery(QueryBuilders.queryStringQuery("服务器").field("content"))
                .get();
        SearchHits hits = searchResponse.getHits();
        System.out.println("查询到的命中数总数量"+hits.getTotalHits());
        //5.遍历结果集
        for (SearchHit hit : hits) {//hit就是一个一个的文档
            String sourceAsString = hit.getSourceAsString();
            System.out.println(sourceAsString);
        }
    }
    //词条查询
    @Test
    public void termQuery(){
        SearchResponse searchResponse = transportClient
                .prepareSearch("blog001")
                .setTypes("article")
                .setQuery(QueryBuilders.termQuery("content","常"))
                .get();
        SearchHits hits = searchResponse.getHits();
        System.out.println("查询到的命中数总数量"+hits.getTotalHits());
        //5.遍历结果集
        for (SearchHit hit : hits) {//hit就是一个一个的文档
            String sourceAsString = hit.getSourceAsString();
            System.out.println(sourceAsString);
        }
    }
    //匹配查询
    @Test
    public void matchQuery(){
        SearchResponse searchResponse = transportClient
                .prepareSearch("blog001")
                .setTypes("article")
                .setQuery(QueryBuilders.matchQuery("id",1))
                .get();
        SearchHits hits = searchResponse.getHits();
        System.out.println("查询到的命中数总数量"+hits.getTotalHits());
        //5.遍历结果集
        for (SearchHit hit : hits) {//hit就是一个一个的文档
            String sourceAsString = hit.getSourceAsString();
            System.out.println(sourceAsString);
        }
    }
    //匹配查询
    @Test
    public void wildCardQuery(){
        SearchResponse searchResponse = transportClient
                .prepareSearch("blog001")
                .setTypes("article")
                //服务*、服？、？
                .setQuery(QueryBuilders.wildcardQuery("content","服*"))
                .get();
        SearchHits hits = searchResponse.getHits();
        System.out.println("查询到的命中数总数量"+hits.getTotalHits());
        //5.遍历结果集
        for (SearchHit hit : hits) {//hit就是一个一个的文档
            String sourceAsString = hit.getSourceAsString();
            System.out.println(sourceAsString);
        }
    }
}
