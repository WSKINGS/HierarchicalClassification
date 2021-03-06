package com.ws.io;

import com.google.gson.Gson;
import com.ws.classifier.SvmClassifier;
import com.ws.model.Feature;
import com.ws.util.FeatureUtil;
import com.ws.util.Parameters;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Created by Administrator on 2015/12/9.
 */
public class HdfsOutputTest implements Serializable {
    private static final String testPath = "hdfs://10.1.0.149:9000/user/wangshuai/tests/";

    private JavaSparkContext jsc;

    @Before
    public void beforeClass() {
        SparkConf conf = new SparkConf().setAppName("testSaveRdd");
        conf.setMaster("local");
        jsc = new JavaSparkContext(conf);
    }

    @Test
    public void testSaveRdd(){
        String[] words = new String[]{"wangshuai","hello","world"};
        List<Feature> list = new ArrayList<Feature>(3);
        for (String word : words) {
            Feature feature = new Feature();
            feature.setWord(word);
            feature.setIndex(1);
            feature.setIdf(1.009);
            feature.setTf(2);
            list.add(feature);
        }
        JavaRDD<Feature> rdd = jsc.parallelize(list);
        rdd.saveAsTextFile(testPath+"testSaveFeatures");
        jsc.close();
    }

    @Test
    public void testLoadFeature(){
        Map<String, Feature> featureMap = FeatureUtil.loadFeatureMap(jsc,testPath+"testSaveFeatures");
        System.out.println(featureMap.size());
    }

    @Test
    public void testSvmModel(){
        Vector pos = Vectors.dense(new double[]{1.0,1.0,1.0,0.0,0.0,0.0});
        Vector neg = Vectors.dense(new double[]{0.0,0.0,0.0,1.0,1.0,1.0});

        LabeledPoint posP = new LabeledPoint(1.0,pos);
        LabeledPoint negP = new LabeledPoint(0.0,neg);

        JavaRDD<LabeledPoint> points = jsc.parallelize(Arrays.asList(posP,negP));
        SvmClassifier classifier = new SvmClassifier();
        SVMModel model = classifier.train(points,10);
        model.save(jsc.sc(),testPath+"testSvmModel");
    }

    @Test
    public void testSvmClassify(){
        SVMModel model = SVMModel.load(jsc.sc(), testPath + "testSvmModel");

        Vector pos = Vectors.dense(new double[]{1.0,1.0,1.0,0.0,0.0,0.0});
        Vector neg = Vectors.dense(new double[]{0.0,0.0,0.0,1.0,1.0,1.0});

        double result = model.predict(pos);
        assertThat(1.0, equalTo(result));

        result = model.predict(neg);
        assertThat(0.0, equalTo(result));
    }

    @Test
    public void saveClassHierarchical(){
        Tuple2<String,Integer> tuple2 = new Tuple2<String, Integer>("word",1);
        System.out.println(tuple2);
    }

    @Test
    public void testLoadModel(){
        SVMModel model = SVMModel.load(jsc.sc(), Parameters.modelPath + "14.03.model");
        assertThat(model, notNullValue());
    }

    @Test
    public void testJavaHdfs() throws IOException {
        String hdfsHost = "hdfs://10.1.0.149:9000";
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(hdfsHost), conf);
        Path path = new Path("/user/wangshuai/test");

        FileStatus[] statuses = fs.listStatus(path);

        for (FileStatus status : statuses) {
            System.out.println(status.getPath().toString());
        }

        fs.close();
    }
}
