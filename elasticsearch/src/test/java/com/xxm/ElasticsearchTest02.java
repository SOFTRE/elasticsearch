package com.xxm;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.xxm.pojo.Article;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
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
 * @CreateDate: 2019-11-19 14:57:56 周二
 * @LastModifyDate:
 * @LastModifyBy:
 * @Version: V1.0
 */
public class ElasticsearchTest02 {
    public TransportClient transportClient;

    @Before
    public void setClient() throws Exception {
        //1.创建客户端对象 连接服务器
        Settings settings = Settings.EMPTY;//(单节点的默认配置)
        transportClient = new PreBuiltTransportClient(settings);
        //2.添加服务器的地址port:指定tcp连接端口9300
        transportClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9300));
    }

    @After
    public void close() {
        //6.关闭客户端对象，释放资源
        transportClient.close();
    }

    //创建索引
    @Test
    public void createIndex() throws Exception {
        transportClient.admin().indices().prepareCreate("blog002").get();
    }

    @Test
    public void deleteIndex() {
        transportClient.admin().indices().prepareDelete("blog002", "blog001").get();
    }

    //手动创建映射
    @Test
    public void method() throws Exception {
        PutMappingRequest putMappingRequest = new PutMappingRequest();
        //1.创建索引
        transportClient.admin().indices().prepareCreate("blog003").get();
        //2.创建映射
        XContentBuilder xcontentbuilder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("article")
                .startObject("properties")
                .startObject("id")
                .field("type", "long")
                .field("index", "true")
                .field("store", "true")
                .endObject()
                .startObject("title")
                .field("type", "text")
                .field("index", "true")
                .field("store", "true")
                .field("analyzer", "ik_smart")//默认是标准分词器
                .endObject()
                .startObject("content")
                .field("type", "text")
                .field("index", "true")
                .field("store", "true")
                .field("analyzer", "ik_smart")//默认是标准分词器
                .endObject()
                .endObject()
                .endObject()
                .endObject();
        putMappingRequest.indices("blog003").type("article").source(xcontentbuilder);
        transportClient.admin().indices().putMapping(putMappingRequest).get();
    }

    @Test
    public void addDocument() {
        Map<String, Object> source = new HashMap<String, Object>();
        source.put("id", 1);
        source.put("title", "elasticsearch的标题");
        source.put("content", "ElasticSearch是一个基于Lucene的搜索服务器。它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web接口。Elasticsearch是用Java开发的，并作为Apache许可条款下的开放源码发布，是当前流行的企业级搜索引擎。设计用于云计算中，能够达到实时搜索，稳定，可靠，快速，安装使用方便。");
        transportClient.prepareIndex("blog003", "article", "1").setSource(source).get();
    }

    //通过Json形式
    @Test
    public void addDocumentByJson() {
        String source = "{\"id\":1,\"title\":\"afafs\"}";
        transportClient.prepareIndex("blog003", "article", "2").setSource(source, XContentType.JSON).get();
    }

    //通过Json-->POJO-->Article的pojo
    //通过Jackson的API
    @Test
    public void addDocumentByArticlePojo() throws Exception {
        for (long i = 0; i < 100; i++) {
            Article article = new Article(i, "ElasticSearch是一个基于Lucene的搜索服务器" + i, "ElasticSearch是一个基于Lucene的搜索服务器。它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web接口。Elasticsearch是用Java开发的，并作为Apache许可条款下的开放源码发布，是当前流行的企业级搜索引擎。设计用于云计算中，能够达到实时搜索，稳定，可靠，快速，安装使用方便。" + i);
            ObjectMapper objectMapper = new ObjectMapper();
            String source = objectMapper.writeValueAsString(article);
            transportClient.prepareIndex("blog003", "article", i + "").setSource(source, XContentType.JSON).get();
        }
    }

    @Test
    public void deleteDocument() throws Exception {
        //transportClient.prepareDelete("blog003","article","3");
        DeleteRequest deleteRequest = new DeleteRequest("blog003", "article", "3");
        transportClient.delete(deleteRequest).get();
    }

    //查询所有
    @Test
    public void matchAll() throws Exception {
        //1.创建客户端对象设置地址
        //2.创建查询对象设置查询条件
        SearchResponse searchResponse = transportClient
                .prepareSearch("blog003")
                .setTypes("article")
                .setQuery(QueryBuilders.matchAllQuery())
                .get();
        //3.获取结果集
        SearchHits hits = searchResponse.getHits();
        System.out.println("命中数:" + hits.getTotalHits());
        ObjectMapper objectMapper = new ObjectMapper();
        //4.遍历结果集
        for (SearchHit hit : hits) {
            String sourceAsString = hit.getSourceAsString();//json的字符串-->document
            //System.out.println(sourceAsString);
            Article article = objectMapper.readValue(sourceAsString, Article.class);
            System.out.println(article);

        }
    }

    //条件查询
    @Test
    public void stringQuery() throws Exception {
        //1.创建客户端对象设置地址
        //2.创建查询对象设置查询条件
        SearchResponse searchResponse = transportClient
                .prepareSearch("blog003")
                .setTypes("article")
                //从所有字段上获取数据
                .setQuery(QueryBuilders.queryStringQuery("服务器"))
                .get();
        //3.获取结果集
        SearchHits hits = searchResponse.getHits();
        System.out.println("命中数:" + hits.getTotalHits());
        ObjectMapper objectMapper = new ObjectMapper();
        //4.遍历结果集
        for (SearchHit hit : hits) {
            String sourceAsString = hit.getSourceAsString();//json的字符串-->document
            //System.out.println(sourceAsString);
            Article article = objectMapper.readValue(sourceAsString, Article.class);
            System.out.println(article);

        }
    }

    //匹配查询 先分词再查询
    @Test
    public void matchQuery() throws Exception {
        //1.创建客户端对象设置地址
        //2.创建查询对象设置查询条件
        SearchResponse searchResponse = transportClient
                .prepareSearch("blog003")
                .setTypes("article")
                //从content字段上获取数据
                .setQuery(QueryBuilders.matchQuery("content", "服"))
                .get();
        //3.获取结果集
        SearchHits hits = searchResponse.getHits();
        System.out.println("命中数:" + hits.getTotalHits());
        ObjectMapper objectMapper = new ObjectMapper();
        //4.遍历结果集
        for (SearchHit hit : hits) {
            String sourceAsString = hit.getSourceAsString();//json的字符串-->document
            //System.out.println(sourceAsString);
            Article article = objectMapper.readValue(sourceAsString, Article.class);
            System.out.println(article);
        }
    }

    //词条查询，整体进行匹配查询
    @Test
    public void termQuery() throws Exception {
        //1.创建客户端对象设置地址
        //2.创建查询对象设置查询条件
        SearchResponse searchResponse = transportClient
                .prepareSearch("blog003")
                .setTypes("article")
                //从content字段上获取数据
                .setQuery(QueryBuilders.termQuery("content", "服务"))
                .get();
        //3.获取结果集
        SearchHits hits = searchResponse.getHits();
        System.out.println("命中数:" + hits.getTotalHits());
        ObjectMapper objectMapper = new ObjectMapper();
        //4.遍历结果集
        for (SearchHit hit : hits) {
            String sourceAsString = hit.getSourceAsString();//json的字符串-->document
            //System.out.println(sourceAsString);
            Article article = objectMapper.readValue(sourceAsString, Article.class);
            System.out.println(article);
        }
    }

    @Test
    public void fuzzyQuery() throws Exception {
        //1.创建客户端对象设置地址
        //2.创建查询对象设置查询条件
        SearchResponse searchResponse = transportClient
                .prepareSearch("blog003")
                .setTypes("article")
                //从content字段上获取数据
                .setQuery(QueryBuilders.fuzzyQuery("content", "elasssticsearch"))
                .get();
        //3.获取结果集
        SearchHits hits = searchResponse.getHits();
        System.out.println("命中数:" + hits.getTotalHits());
        ObjectMapper objectMapper = new ObjectMapper();
        //4.遍历结果集
        for (SearchHit hit : hits) {
            String sourceAsString = hit.getSourceAsString();//json的字符串-->document
            //System.out.println(sourceAsString);
            Article article = objectMapper.readValue(sourceAsString, Article.class);
            System.out.println(article);
        }
    }

    @Test
    public void rangeQuery() throws Exception {
        //1.创建客户端对象设置地址
        //2.创建查询对象设置查询条件
        SearchResponse searchResponse = transportClient
                .prepareSearch("blog003")
                .setTypes("article")
                //从content字段上获取数据
                //.setQuery(QueryBuilders.rangeQuery("id").gt(0).lte(2))
                .setQuery(QueryBuilders.rangeQuery("id").from(0, true).to(2))
                .get();
        //3.获取结果集
        SearchHits hits = searchResponse.getHits();
        System.out.println("命中数:" + hits.getTotalHits());
        ObjectMapper objectMapper = new ObjectMapper();
        //4.遍历结果集
        for (SearchHit hit : hits) {
            String sourceAsString = hit.getSourceAsString();//json的字符串-->document
            //System.out.println(sourceAsString);
            Article article = objectMapper.readValue(sourceAsString, Article.class);
            System.out.println(article);
        }
    }

    //多条件组合查询
    //1.一定要满足条件1 title:elasticsearch
    //2.一定要满足条件2 id在0-20区间
    @Test
    public void boolQuery() throws Exception {
        //创建一个bool查询对象
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        //条件must 必须满足 AND
        //条件should 要改满足 OR
        //条件must_not 必须不满足 NOT
        //条件filter也是必须满足 AND
        //组合条件1
        //boolQuery.must(QueryBuilders.termQuery("title","elasticsearch"));
        //boolQuery.mustNot(QueryBuilders.rangeQuery("id").from(0,true).to(20,true));
        boolQuery.filter(QueryBuilders.termQuery("title", "elasticsearch"));
        boolQuery.filter(QueryBuilders.rangeQuery("id").from(0, true).to(20, true));
        //组合条件2
        //1.创建客户端对象设置地址
        //2.创建查询对象设置查询条件
        SearchResponse searchResponse = transportClient
                .prepareSearch("blog003")
                .setTypes("article")
                //从content字段上获取数据
                //.setQuery(QueryBuilders.rangeQuery("id").gt(0).lte(2))
                .setQuery(boolQuery)
                .get();
        //3.获取结果集
        SearchHits hits = searchResponse.getHits();
        System.out.println("命中数:" + hits.getTotalHits());
        ObjectMapper objectMapper = new ObjectMapper();
        //4.遍历结果集
        for (SearchHit hit : hits) {
            String sourceAsString = hit.getSourceAsString();//json的字符串-->document
            //System.out.println(sourceAsString);
            Article article = objectMapper.readValue(sourceAsString, Article.class);
            System.out.println(article);
        }
    }

    @Test
    public void wildCardQuery() throws Exception {
        //1.创建客户端对象设置地址
        //2.创建查询对象设置查询条件
        SearchResponse searchResponse = transportClient
                .prepareSearch("blog003")
                .setTypes("article")
                //从content字段上获取数据
                .setQuery(QueryBuilders.wildcardQuery("content", "服务?"))//服*
                .get();
        //3.获取结果集
        SearchHits hits = searchResponse.getHits();
        System.out.println("命中数:" + hits.getTotalHits());
        ObjectMapper objectMapper = new ObjectMapper();
        //4.遍历结果集
        for (SearchHit hit : hits) {
            String sourceAsString = hit.getSourceAsString();//json的字符串-->document
            //System.out.println(sourceAsString);
            Article article = objectMapper.readValue(sourceAsString, Article.class);
            System.out.println(article);
        }
    }

    //分页查询
    @Test
    public void pageQuery() throws Exception {
        //1.创建客户端对象设置地址
        //2.创建查询对象设置查询条件
        SearchResponse searchResponse = transportClient
                .prepareSearch("blog003")
                .setTypes("article")
                //从content字段上获取数据
                .setQuery(QueryBuilders.termQuery("content", "服务器"))//服*
                .setFrom(0)//开始分页的位置（page-1）*rows
                .setSize(20)//每页显示的行 rows
                .get();
        //3.获取结果集
        SearchHits hits = searchResponse.getHits();
        System.out.println("命中数:" + hits.getTotalHits());
        ObjectMapper objectMapper = new ObjectMapper();
        //4.遍历结果集
        for (SearchHit hit : hits) {
            String sourceAsString = hit.getSourceAsString();//json的字符串-->document
            //System.out.println(sourceAsString);
            Article article = objectMapper.readValue(sourceAsString, Article.class);
            System.out.println(article);
        }
    }

    //分页查询
    @Test
    public void orderQuery() throws Exception {//select * from order by id asc
        //1.创建客户端对象设置地址
        //2.创建查询对象设置查询条件
        SearchResponse searchResponse = transportClient
                .prepareSearch("blog003")
                .setTypes("article")
                //从content字段上获取数据
                .setQuery(QueryBuilders.termQuery("content", "服务器"))//服*
                .setFrom(0)//开始分页的位置（page-1）*rows
                .setSize(20)//每页显示的行 rows
                .addSort("id", SortOrder.DESC)
                .get();
        //3.获取结果集
        SearchHits hits = searchResponse.getHits();
        System.out.println("命中数:" + hits.getTotalHits());
        ObjectMapper objectMapper = new ObjectMapper();
        //4.遍历结果集
        for (SearchHit hit : hits) {
            String sourceAsString = hit.getSourceAsString();//json的字符串-->document
            //System.out.println(sourceAsString);
            Article article = objectMapper.readValue(sourceAsString, Article.class);
            System.out.println(article);
        }
    }

    //高亮查询
    //根据查询的结果,搜索的文本 都高亮显示.
    @Test
    public void highlighter() throws Exception {

        //1.开启高亮功能
        //2.设置高亮显示的字段
        //3.设置高亮的前缀和后缀 (设置style)

        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.field("title");//高亮titile字段

        highlightBuilder.preTags("<em style=\"color:red\">");//前缀      //<em style="color:red">lucene</em>is a ll
        highlightBuilder.postTags("</em>");//后缀
        SearchResponse response = transportClient.prepareSearch("blog003")
                .setTypes("article")
                .setQuery(QueryBuilders.termQuery("title", "服务器"))
                .highlighter(highlightBuilder)//开启了高亮
                .get();

        SearchHits hits = response.getHits();//获取结果

        System.out.println("总记录数:" + hits.getTotalHits());

        ObjectMapper objectMapper = new ObjectMapper();

        for (SearchHit hit : hits) {

            //获取高亮数据
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();//key:高亮显示的字段名 value:高亮字段所对应的所有的额高亮数据
            //获取title的高亮数据
            HighlightField highlightField = highlightFields.get("title");//参数名一定要 设置高亮的字段的名称一致.
            //获取高亮的碎片
            Text[] fragments = highlightField.fragments();
            StringBuffer stringBuffer = new StringBuffer();
            for (Text fragment : fragments) {
                String string = fragment.string();//高亮的数据
                //合并碎片
                stringBuffer.append(string);//
            }
            System.out.println("高亮的数据:" + stringBuffer);

            //设置
            Article article = objectMapper.readValue(hit.getSourceAsString(), Article.class);


            article.setTitle(stringBuffer.toString());

            System.out.println(article);//目前没有高亮的数据
        }
    }
}
