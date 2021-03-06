package com.ws.application;

import com.ws.classifier.NewsReportTransformation;
import com.ws.classifier.SvmClassifier;
import com.ws.io.ContentProvider;
import com.ws.io.HdfsContentProvider;
import com.ws.model.InputRequest;
import com.ws.model.NewsReport;
import com.ws.util.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

/**
 * Created by Administrator on 2015/10/27.
 */
public class TrainModel implements Serializable {
    private static final long serialVersionUID = -6327540086331827844L;

    public static void main(String[] args) throws IOException {
        SparkConf conf = new SparkConf()
                .setAppName("trainModel");
        if (args.length > 0 && args[0] != null) {
            conf.setMaster(args[0]);
        } else {
            conf.setMaster("local");
//            conf.setMaster("spark://10.1.0.149:7077");
        }

        JavaSparkContext jsc = new JavaSparkContext(conf);
        InputRequest request = new InputRequest();
        request.setJsc(jsc);
        request.setFilepath(Parameters.hdfsHost+Parameters.filepath);

        //加载训练集
        ContentProvider contentProvider = new HdfsContentProvider();
//        ContentProvider contentProvider = new FileContentProvider();

        JavaRDD<NewsReport> src = contentProvider.getSource(request);

        //统计每个类别的数目
        //ClassCounter classCounter = new ClassCounter();
        //JavaPairRDD<String, Integer> classCountRdd = classCounter.countClassNum(src);
        //Map<String,Integer> classCountMap = classCountRdd.collectAsMap();
        //Map<String,List<String>> hierarchical = getHierarchicalMap(classCountMap);
        Map<String,Iterable<String>> hierarchical = ClassHierarchicalUtils.loadClassHierarchical(jsc);


        //加载向量空间
        //Map<String,Feature> featureMap = FeatureUtil.loadFeatureMap(jsc, Parameters.featurePath);
        //将新闻用向量表示 key:cat_doc
        final JavaPairRDD<String, Vector> docVectorRdd = NewsReportTransformation.mapNewsReport2Vector(jsc, src);

        docVectorRdd.cache();
        for (String father : hierarchical.keySet()) {
            SVMModel model = trainModelByType(docVectorRdd,father);
            HdfsUtils.safeSaveModel(model, jsc.sc(),Parameters.modelPath+father+".model");
            //model.save(jsc.sc(),Parameters.modelPath+father+".model");
        }
        for (final String father : hierarchical.keySet()) {
            Iterable<String> children = hierarchical.get(father);
            JavaPairRDD<String, Vector> sampleRdd = docVectorRdd.filter(new Function<Tuple2<String, Vector>, Boolean>() {
                public Boolean call(Tuple2<String, Vector> class_doc2vector) throws Exception {
                    String key = class_doc2vector._1;
                    if (key.startsWith(father)) {
                        return true;
                    }
                    return false;
                }
            });

            for (String child : children) {
                SVMModel model = trainModelByType(sampleRdd, child);
                HdfsUtils.safeSaveModel(model,jsc.sc(), Parameters.modelPath + child + ".model");
                //model.save(jsc.sc(), Parameters.modelPath + child + ".model");
            }
        }
    }

    private static Map<String, List<String>> getHierarchicalMap(Map<String, Integer> classCountMap) {
        Map<String,List<String>> hierarchicalMap = new HashMap<String, List<String>>();
        for (String type:classCountMap.keySet()){
            if (type.contains(".")){
                String father = type.split("[.]")[0];
                if (!hierarchicalMap.containsKey(father)){
                    hierarchicalMap.put(father,new ArrayList<String>());
                }
                hierarchicalMap.get(father).add(type);
            } else {
                if (!hierarchicalMap.containsKey(type)) {
                    hierarchicalMap.put(type,new ArrayList<String>());
                }
            }
        }
        return hierarchicalMap;
    }

    private static SVMModel trainModelByType(JavaPairRDD<String, Vector> docVectorRdd, final String type){

        JavaRDD<LabeledPoint> points = docVectorRdd.map(new Function<Tuple2<String, Vector>, LabeledPoint>() {
            public LabeledPoint call(Tuple2<String, Vector> stringVectorTuple2) throws Exception {
                String docType = stringVectorTuple2._1.split("_")[0];
                if (docType.startsWith(type)){
                    return new LabeledPoint(1.0,stringVectorTuple2._2);
                }
                return new LabeledPoint(0.0,stringVectorTuple2._2);
            }
        });

       // List<LabeledPoint> temp = points.collect();
        SvmClassifier classifier = new SvmClassifier();
        return classifier.train(points,10);
    }
}
