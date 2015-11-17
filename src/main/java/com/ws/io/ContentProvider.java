package com.ws.io;

import com.ws.model.NewsReport;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;

/**
 * Created by Administrator on 2015/10/27.
 */
public interface ContentProvider extends Serializable {
    JavaRDD<NewsReport> getSource(JavaSparkContext jsc);
}
