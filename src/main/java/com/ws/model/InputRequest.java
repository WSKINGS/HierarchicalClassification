package com.ws.model;

import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;

/**
 * Created by Administrator on 2015/11/26.
 */
public class InputRequest implements Serializable {
    private static final long serialVersionUID = -1178233879571090389L;
    private JavaSparkContext jsc;
    private String filepath;

    public JavaSparkContext getJsc() {
        return jsc;
    }

    public void setJsc(JavaSparkContext jsc) {
        this.jsc = jsc;
    }

    public String getFilepath() {
        return filepath;
    }

    public void setFilepath(String filepath) {
        this.filepath = filepath;
    }
}
