package com.ws.application;

import com.ws.classifier.NewsReportTransformation;
import com.ws.classifier.SvmClassifier;
import com.ws.io.ContentProvider;
import com.ws.io.FileContentProvider;
import com.ws.io.HdfsContentProvider;
import com.ws.model.Feature;
import com.ws.model.InputRequest;
import com.ws.model.NewsReport;
import com.ws.process.ClassCounter;
import com.ws.process.VectorSpaceGenerator;
import com.ws.util.FeatureUtil;
import com.ws.util.Segment;
import com.ws.util.StopWords;
import org.ansj.domain.Term;
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
    private static final String filepath="hdfs://10.1.0.149:9000/user/wangshuai/train.json";
//    private static final String filepath = "D:\\temp\\train.trs2.xml";
    private static final String modelPath="hdfs://10.1.0.149:9000/user/wangshuai/model/";
    private static final String featurePath="hdfs://10.1.0.149:9000/user/wangshuai/features";

    //private static final String featurePath = "d:\\temp\\features";

    private static final long serialVersionUID = -6327540086331827844L;
    private static final int dfThreshold = 2;

    public static void main(String[] args) throws IOException {
        SparkConf conf = new SparkConf()
                .setAppName("classification")
                .set("spark.executor.memory","6g")
                .set("spark.driver.memory","4g");
        if (args.length > 0 && args[0] != null) {
            conf.setMaster(args[0]);
        } else {
            conf.setMaster("local");
//            conf.setMaster("spark://10.1.0.149:7077");
        }

        JavaSparkContext jsc = new JavaSparkContext(conf);
        InputRequest request = new InputRequest();
        request.setJsc(jsc);
        request.setFilepath(filepath);

        //加载训练集
        ContentProvider contentProvider = new HdfsContentProvider();
//        ContentProvider contentProvider = new FileContentProvider();
        JavaRDD<NewsReport> src = contentProvider.getSource(request);

        //统计每个类别的数目
        ClassCounter classCounter = new ClassCounter();
        JavaPairRDD<String, Integer> classCountRdd = classCounter.countClassNum(src);
        Map<String,Integer> classCountMap = classCountRdd.collectAsMap();
        Map<String,List<String>> hierarchical = getHierarchicalMap(classCountMap);


        //加载向量空间
        Map<String,Feature> featureMap = FeatureUtil.loadFeatureMap(jsc, featurePath);
        //将新闻用向量表示 key:cat_doc
        final JavaPairRDD<String, Vector> docVectorRdd = NewsReportTransformation.mapNewsReport2Vector(src, featureMap);

        docVectorRdd.cache();
        for (String father : hierarchical.keySet()) {
            SVMModel model = trainModelByType(docVectorRdd,father);
            model.save(jsc.sc(),modelPath+father+".model");
        }
        for (final String father : hierarchical.keySet()) {
            List<String> children = hierarchical.get(father);
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
                model.save(jsc.sc(), modelPath + child + ".model");
            }
        }
        //SVMModel model = trainModelByType(docVectorRdd,"14.18");
        //testClassify(model,spaceMap,idfMap);
        //model.save(jsc.sc(),modelPath+"14.18.model");
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

    private static void testClassify(SVMModel model, Map<String, Integer> spaceMap, Map<String, Double> idfMap) {
        NewsReport news = new NewsReport();
        news.setTitle("市革命烈士陵园墓包将作保护改造");
        news.setContent("本报讯记者从市民政部门获悉，为深切缅怀汕头市在各个革命历史时期为国捐躯的烈士们，更好地展现烈士墓风采，" +
                "汕头市将对汕头市革命烈士陵园墓包进行保护改造。 \n" +
                "汕头市革命烈士陵园位于\uE40B石风景区，始建于1959年4月。1961年6月，烈士墓包、休息室、拱桥、溢洪坝和道池落成。" +
                "该烈士墓包是全省唯一一座墓包形状的纪念建筑物，特色明显。由于建设年代久远，长期经受风雨侵蚀，墓包主体渗漏，为适用祭扫革命烈士墓活动的需要，" +
                "亟需对烈士墓包进行改造和设计。 \n" +
                "据悉，此次改造将采取“修旧如旧”的方式，既要突出历史的纪念意义及陵园文化底蕴相融合，又要具有时代特色；总体风格将保持庄严、" +
                "古朴、新颖；烈士墓包的材质、颜色、造型等要与陵园景观环境相协调。改造工程将广泛听取社会各界意见，近日市民政局已向全市征求汕头市革命烈士陵园墓包保护改造设计方案" +
                "及建议，包括平面布局、立面设计和整体实施效果等，有意者可将设计方案和建议送市民政局优抚安置科。（李凯）");

        Vector v = NewsReportTransformation.mapNewsReport2Vector(news, spaceMap, idfMap);
        double score = model.predict(v);
        System.out.println("score:"+score);

        news.setTitle("尼勒克短信传递造林进度");
        news.setContent("本报讯　“截至4月6日，尼勒克县共完成人工造林3.58万亩。造林\n" +
                "进度较快的乡有……”4月7日下午，新疆维吾尔自治区伊犁州林业局局\n" +
                "领导，以及各处室负责人的手机收到全县造林进度的短信。今年，伊犁\n" +
                "州给尼勒克县下达的人工造林任务为2.4万亩，而县里自定的任务是4万\n" +
                "亩。全县70多个县直机关、企事业单位2300人，奔赴全县12个乡（镇、\n" +
                "场），完成1.4万亩的造林任务，占全县总任务量的三成以上。截至目\n" +
                "前，全县已完成造林3.58万亩。（刘河新;曹爱新）");
        v = NewsReportTransformation.mapNewsReport2Vector(news,spaceMap,idfMap);
        score = model.predict(v);
        System.out.println("score2:"+score);
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
