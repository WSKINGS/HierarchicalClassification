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
    private String ccnc_cat;
    private String ccnc_label;

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

    public String getCcnc_cat() {
        return ccnc_cat;
    }

    public void setCcnc_cat(String ccnc_cat) {
        this.ccnc_cat = ccnc_cat;
    }

    public String getCcnc_label() {
        return ccnc_label;
    }

    public void setCcnc_label(String ccnc_label) {
        this.ccnc_label = ccnc_label;
    }
}
