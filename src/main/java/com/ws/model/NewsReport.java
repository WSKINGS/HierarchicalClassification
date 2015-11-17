package com.ws.model;

import java.io.Serializable;

/**
 * Created by Administrator on 2015/10/27.
 */
public class NewsReport implements Serializable {
    private static final long serialVersionUID = -3455743193607925429L;
    private String id;
    private String title;
    private String content;
    private String catId;
    private String catLable;

    public String getId() {
        return id;
    }

    public void setId(String id) {
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

    public String getCatId() {
        return catId;
    }

    public void setCatId(String catId) {
        this.catId = catId;
    }

    public String getCatLable() {
        return catLable;
    }

    public void setCatLable(String catLable) {
        this.catLable = catLable;
    }
}
