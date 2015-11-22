package com.ws.application;

import com.ws.model.NewsReport;
import org.apache.spark.api.java.JavaRDD;

import java.io.Serializable;

/**
 * Created by Administrator on 2015/11/22.
 */
public class Classifier implements Serializable {
    private static final long serialVersionUID = 2762959130464278270L;

    public String classify(NewsReport newsReport) {
        return null;
    }

    public JavaRDD<NewsReport> classify(JavaRDD<NewsReport> srcRdd) {

        return null;
    }
}
