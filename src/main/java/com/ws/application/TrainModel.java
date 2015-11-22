package com.ws.application;

import com.ws.classifier.NewsReportTransformation;
import com.ws.classifier.SvmClassifier;
import com.ws.io.ContentProvider;
import com.ws.io.FileContentProvider;
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
import org.apache.spark.mllib.linalg.Vectors;
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
    private static final int dfThreshold = 4;

    public static void main(String[] args) throws IOException {
        SparkConf conf = new SparkConf().setAppName("classification");
        if (args.length > 0 && args[0] != null) {
            conf.setMaster(args[0]);
        } else {
            conf.setMaster("local");
        }

        JavaSparkContext jsc = new JavaSparkContext(conf);

        //加载训练集
        ContentProvider contentProvider = new FileContentProvider();
        JavaRDD<NewsReport> src = contentProvider.getSource(jsc);

        SVMModel model = trainModelByType(src,"14.18");
        model.save(jsc.sc(),"model/14.18.model");
    }

    private static SVMModel trainModelByType(JavaRDD<NewsReport> src, final String type){
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

        final Map<String, Integer> dfMap = spaceRdd.collectAsMap();
        final Map<String, Integer> spaceMap = changeSpaceRdd2Map(spaceRdd);

        final JavaPairRDD<String, Vector> docVectorRdd = mapWords2Vector(wordsOfNews, spaceMap, dfMap);


        JavaRDD<LabeledPoint> points = docVectorRdd.map(new Function<Tuple2<String, Vector>, LabeledPoint>() {
            public LabeledPoint call(Tuple2<String, Vector> stringVectorTuple2) throws Exception {
                String docType = stringVectorTuple2._1.split("_")[0];
                if (type.equals(docType)){
                    return new LabeledPoint(1.0,stringVectorTuple2._2);
                }
                return new LabeledPoint(0.0,stringVectorTuple2._2);
            }
        });

        SvmClassifier classifier = new SvmClassifier();
        SVMModel model = classifier.train(points,10);
        return model;
    }

    private static JavaPairRDD<String, Vector> mapWords2Vector(JavaPairRDD<String, Integer> wordsOfNews, final Map<String, Integer> spaceMap, final Map<String, Integer> dfMap) {
        JavaPairRDD<String, String> docWordRDD = wordsOfNews.mapToPair(new PairFunction<Tuple2<String, Integer>, String, String>() {
            public Tuple2<String, String> call(Tuple2<String, Integer> tuple2) throws Exception {
                int index = tuple2._1.lastIndexOf("_");
                String key = tuple2._1.substring(0, index);
                String value = tuple2._1.substring(index + 1) + ":" + tuple2._2;
                return new Tuple2<String, String>(key, value);
            }
        });

        JavaPairRDD<String, Iterable<String>> docRdd = docWordRDD.groupByKey();

        final long count = docRdd.count();

        JavaPairRDD<String, Vector> vectorRdd = docRdd.mapToPair(new PairFunction<Tuple2<String, Iterable<String>>, String, Vector>() {
            public Tuple2<String, Vector> call(Tuple2<String, Iterable<String>> stringIterableTuple2) throws Exception {
                Iterator<String> iterator = stringIterableTuple2._2.iterator();
                TreeMap<Integer, Double> map = new TreeMap<Integer, Double>();
                while (iterator.hasNext()) {
                    //str format: word:tf
                    String str = iterator.next();
                    String word = str.split(":")[0];

                    if (!spaceMap.containsKey(word)) {
                        continue;
                    }
                    double tf = Double.parseDouble(str.split(":")[1]);

                    int index = spaceMap.get(word);
                    double tfidf = tf * Math.log((count + 1.0) / (dfMap.get(word) + 1.0));
                    map.put(index,tfidf);
                }

                int[] indices = new int[map.size()];
                double[] weights = new double[map.size()];
                int num = 0;
                for (Integer key : map.keySet()){
                    indices[num] = key;
                    weights[num] = map.get(key);
                    num++;
                }
                Vector vector = Vectors.sparse(num,indices,weights);
                return new Tuple2<String, Vector>(stringIterableTuple2._1,vector);
            }
        });

        return vectorRdd;
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
