package com.ws.application;

import com.ws.io.ContentProvider;
import com.ws.io.HdfsContentProvider;
import com.ws.model.Feature;
import com.ws.model.InputRequest;
import com.ws.model.NewsReport;
import com.ws.process.ClassCounter;
import com.ws.util.FeatureUtil;
import com.ws.util.Parameters;
import com.ws.util.SparkContextUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Created by Administrator on 2015/12/9.
 */
public class VectorSpaceGenerator implements Serializable {

    private static final long serialVersionUID = -8861596138761922298L;

    public static void main ( String[] args ) {

        //JavaSparkContext jsc = SparkContextUtil.getSparkContext("generateVectorSpace");
        SparkConf conf = new SparkConf().setAppName("generateVectorSpace");
        if (args.length > 0 && args[0] != null) {
            conf.setMaster(args[0]);
        } else {
            conf.setMaster(Parameters.masterUrl);
        }
        //conf.set("spark.executor.memory","7G");

        JavaSparkContext jsc = new JavaSparkContext(conf);
        InputRequest request = new InputRequest();
        request.setJsc(jsc);
        request.setFilepath(Parameters.filepath);

        //加载训练集
        ContentProvider contentProvider = new HdfsContentProvider();
//        ContentProvider contentProvider = new FileContentProvider();
        JavaRDD<NewsReport> src = contentProvider.getSource(request);
//                .filter(new Function<NewsReport, Boolean>() {
//                    public Boolean call(NewsReport newsReport) throws Exception {
//                        if ("39.15".equals(newsReport.getCcnc_cat()) || "14.03".equals(newsReport.getCcnc_cat())
//                                ||"39.11".equals(newsReport.getCcnc_cat()) || "14.15".equals(newsReport.getCcnc_cat())) {
//                            return true;
//                        }
//                        return false;
//                    }
//                });


        //统计每个类别的数目
        ClassCounter classCounter = new ClassCounter();
        JavaPairRDD<String, Integer> classCountRdd = classCounter.countClassNum(src);
        classCountRdd.saveAsTextFile(Parameters.classPath);

        //生成向量空间
        com.ws.process.VectorSpaceGenerator spaceGenerator = new com.ws.process.VectorSpaceGenerator();
        List<Feature> featureList = spaceGenerator.generateVectorSpace(src,classCountRdd);
        FeatureUtil.saveFeatures(jsc, featureList);
    }
}
