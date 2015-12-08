package com.ws.application;

import com.ws.classifier.NewsReportTransformation;
import com.ws.classifier.SvmClassifier;
import com.ws.io.ContentProvider;
import com.ws.io.FileContentProvider;
import com.ws.io.HdfsContentProvider;
import com.ws.model.InputRequest;
import com.ws.model.NewsReport;
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
    //private static final String filepath = "D:\\temp\\train.trs.xml";
    private static final String modelPath="hdfs://10.1.0.149:9000/user/wangshuai/model/";

    private static final long serialVersionUID = -6327540086331827844L;
    private static final int dfThreshold = 2;

    public static void main(String[] args) throws IOException {
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
        request.setFilepath(filepath);

        //加载训练集
        ContentProvider contentProvider = new HdfsContentProvider();
        JavaRDD<NewsReport> src = contentProvider.getSource(request);

        final long totalDocCount = src.count();
        JavaPairRDD<String,Integer> wordsOfNews = changeNewsReport2Dictionary(src);
        wordsOfNews.cache();

        JavaPairRDD<String, Integer> dfRdd = getDfRdd(wordsOfNews);

        JavaPairRDD<String, Integer> spaceRdd = dfRdd.filter(new Function<Tuple2<String, Integer>, Boolean>() {
            public Boolean call(Tuple2<String, Integer> tuple2) throws Exception {
                if (tuple2._2 < dfThreshold) {
                    return false;
                }
                return true;
            }
        });

        final Map<String, Double> idfMap = changeDFRdd2Idf(spaceRdd,totalDocCount);
        final Map<String, Integer> spaceMap = changeSpaceRdd2Map(spaceRdd);

        final JavaPairRDD<String, Vector> docVectorRdd = NewsReportTransformation.mapWords2Vector(wordsOfNews, spaceMap, idfMap);

        SVMModel model = trainModelByType(docVectorRdd,"14.18");


        testClassify(model,spaceMap,idfMap);

        model.save(jsc.sc(),modelPath+"14.18.model");
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
        SVMModel model = classifier.train(points,10);
        return model;
    }

    private static Map<String, Double> changeDFRdd2Idf(JavaPairRDD<String, Integer> spaceRdd, final long totalDocCount) {
        JavaPairRDD<String, Double> idfRdd = spaceRdd.mapToPair(new PairFunction<Tuple2<String, Integer>, String, Double>() {
            public Tuple2<String, Double> call(Tuple2<String, Integer> tuple2) throws Exception {
                double idf = Math.log((totalDocCount + 1.0) / (tuple2._2 + 1.0));
                return new Tuple2<String, Double>(tuple2._1, idf);
            }
        });
        return idfRdd.collectAsMap();
    }


    private static JavaPairRDD<String, Integer> getDfRdd(JavaPairRDD<String, Integer> wordsOfNews) {
        JavaPairRDD<String,Integer> wordsRdd = wordsOfNews.mapToPair(new PairFunction<Tuple2<String, Integer>, String, Integer>() {
            public Tuple2<String, Integer> call(Tuple2<String, Integer> tuple2) throws Exception {
                String word = tuple2._1.split("_")[2];
                return new Tuple2<String, Integer>(word,1);
            }
        });

        JavaPairRDD<String, Integer> dfRdd = wordsRdd.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer+integer2;
            }
        });
        return dfRdd;
    }

    private static JavaPairRDD<String, Integer> changeNewsReport2Dictionary(JavaRDD<NewsReport> src) {
        //分词，去除停用词
        JavaPairRDD<String, Integer> segWordsRdd = src.flatMapToPair(new PairFlatMapFunction<NewsReport, String, Integer>() {
            public Iterable<Tuple2<String, Integer>> call(NewsReport newsReport) throws Exception {
                List<Term> terms = Segment.segNewsreport(newsReport);
                List<Tuple2<String, Integer>> words = new ArrayList<Tuple2<String, Integer>>(terms.size());
                for (Term term : terms) {
                    //去除停用词
                    if (StopWords.isStopWord(term.getName())) {
                        continue;
                    }
                    //key : catId_newsId_word
                    String key = newsReport.getCatId() + "_" + newsReport.getId() + "_" + term.getName();
                    words.add(new Tuple2<String, Integer>(key, 1));
                }
                return words;
            }
        });

        //统计每篇新闻中TF
        JavaPairRDD<String, Integer> tfRdd = segWordsRdd.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        return tfRdd;
    }

    private static Map<String,Integer> changeSpaceRdd2Map(JavaPairRDD<String, Integer> dfRdd) {
        Map<String,Integer> origin = dfRdd.collectAsMap();
        Map<String,Integer> map = new HashMap<String, Integer>(origin.size());
        int index = 0;
        for (String key : origin.keySet()){
            map.put(key,index);
            index++;
        }
        return map;
    }
}
