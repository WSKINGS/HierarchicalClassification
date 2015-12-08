package com.ws.io;

import com.ws.model.InputRequest;
import com.ws.model.NewsReport;
import org.apache.spark.api.java.JavaRDD;

import java.io.Serializable;

/**
 * Created by Administrator on 2015/10/27.
 */
public interface ContentProvider extends Serializable {
    JavaRDD<NewsReport> getSource(InputRequest request);
}
