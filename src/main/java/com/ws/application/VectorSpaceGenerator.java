package com.ws.application;

import com.ws.io.ContentProvider;
import com.ws.io.HdfsContentProvider;
import com.ws.model.Feature;
import com.ws.model.InputRequest;
import com.ws.model.NewsReport;
import com.ws.process.ClassCounter;
import com.ws.util.FeatureUtil;
import com.ws.util.Parameters;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.List;
import java.util.Map;

/**
 * Created by Administrator on 2015/12/9.
 */
public class VectorSpaceGenerator {

    public static void main ( String[] args ) {
        SparkConf conf = new SparkConf().setAppName("classification");
        if (args.length > 0 && args[0] != null) {
            conf.setMaster(args[0]);
        } else {
            conf.setMaster("local");
//            conf.setMaster("spark://10.1.0.149:7077");
        }

        JavaSparkContext jsc = new JavaSparkContext(conf);
        InputRequest request = new InputRequest();
        request.setJsc(jsc);
        request.setFilepath(Parameters.filepath);

        //����ѵ����
        ContentProvider contentProvider = new HdfsContentProvider();
//        ContentProvider contentProvider = new FileContentProvider();
        JavaRDD<NewsReport> src = contentProvider.getSource(request)
                .filter(new Function<NewsReport, Boolean>() {
                    public Boolean call(NewsReport newsReport) throws Exception {
                        if ("39.15".equals(newsReport.getCcnc_cat()) || "14.03".equals(newsReport.getCcnc_cat())
                                ||"39.11".equals(newsReport.getCcnc_cat()) || "14.15".equals(newsReport.getCcnc_cat())) {
                            return true;
                        }
                        return false;
                    }
                });


        //ͳ��ÿ��������Ŀ
        ClassCounter classCounter = new ClassCounter();
        JavaPairRDD<String, Integer> classCountRdd = classCounter.countClassNum(src);
        classCountRdd.saveAsTextFile(Parameters.classPath);

        //���������ռ�
        com.ws.process.VectorSpaceGenerator spaceGenerator = new com.ws.process.VectorSpaceGenerator();
        List<Feature> featureList = spaceGenerator.generateVectorSpace(src,classCountRdd);
        FeatureUtil.saveFeatures(jsc, featureList);
    }
}