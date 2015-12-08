package com.ws.io;

import com.ws.model.InputRequest;
import com.ws.model.NewsReport;
import com.ws.util.NewsReportUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.io.Serializable;

/**
 * Created by Administrator on 2015/11/25.
 */
public class HdfsContentProvider implements ContentProvider,Serializable {
    private static final long serialVersionUID = 7071912222271417061L;
//    private static final String filepath="filepathhdfs://10.1.0.149:9000/user/wangshuai/train";

    public JavaRDD<NewsReport> getSource(InputRequest request) {
        JavaRDD<String> src = request.getJsc().textFile(request.getFilepath());

        JavaRDD<NewsReport> newsRdd = src.map(new Function<String, NewsReport>() {
            public NewsReport call(String s) throws Exception {
                return NewsReportUtil.fromJsonToNewsReport(s);
            }
        });

        return newsRdd;
    }
}
