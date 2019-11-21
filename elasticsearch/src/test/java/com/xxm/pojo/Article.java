package com.xxm.pojo;

import java.io.Serializable;

/**
 * @Program: IntelliJ IDEA elasticsearch
 * @Description: TODO
 * @Author: Mr Liu
 * @Creed: Talk is cheap,show me the code
 * @CreateDate: 2019-11-19 16:08:46 周二
 * @LastModifyDate:
 * @LastModifyBy:
 * @Version: V1.0
 */
public class Article implements Serializable {
    private long id;
    private String title;
    private String content;

    public Article() {
    }

    public Article(long id, String title, String content) {
        this.id = id;
        this.title = title;
        this.content = content;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    @Override
    public String toString() {
        return "Article{" +
                "id=" + id +
                ", title='" + title + '\'' +
                ", content='" + content + '\'' +
                '}';
    }
}
