package com.ws.application;

import com.ws.classifier.SvmClassifier;
import com.ws.io.ContentProvider;
import com.ws.io.HdfsContentProvider;
import com.ws.model.InputRequest;
import com.ws.model.NewsReport;
import com.ws.util.HdfsUtils;
import com.ws.util.Parameters;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.io.Serializable;

/**
 * Created by Administrator on 2015/12/10.
 */
public class Classify implements Serializable {
    private static final long serialVersionUID = -2358078115348425303L;

    public static void main ( String[] args ) {
        SparkConf conf = new SparkConf()
                .setAppName("classification")
                .set("spark.executor.memory", "6g")
                .set("spark.driver.memory", "4g");
        if (args.length > 0 && args[0] != null) {
            conf.setMaster(args[0]);
        } else {
            conf.setMaster("local");
//            conf.setMaster("spark://10.1.0.149:7077");
        }

        JavaSparkContext jsc = new JavaSparkContext(conf);
        InputRequest request = new InputRequest();
        request.setJsc(jsc);
        request.setFilepath(Parameters.testPath);

        //加载训练集
        ContentProvider contentProvider = new HdfsContentProvider();
//        ContentProvider contentProvider = new FileContentProvider();
        JavaRDD<NewsReport> src = contentProvider.getSource(request);

        SvmClassifier classifier = new SvmClassifier();
        JavaPairRDD<String, String> result = classifier.predict(jsc, src);

        HdfsUtils.safeSave(result, Parameters.testResult);
    }

}
