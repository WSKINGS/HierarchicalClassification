package com.ws.classifier;

import com.ws.model.Feature;
import com.ws.model.NewsReport;
import com.ws.util.FeatureUtil;
import com.ws.util.Parameters;
import com.ws.util.Segment;
import com.ws.util.StopWords;
import org.ansj.domain.Term;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

/**
 * Created by Administrator on 2015/11/18.
 */
public class NewsReportTransformation implements Serializable {
    private static final long serialVersionUID = 2255395655953170782L;

    private static Map<String, Feature> featureMap = null;

    public static JavaPairRDD<String, Vector> mapNewsReport2Vector(JavaSparkContext jsc, JavaRDD<NewsReport> newsRdd) {
        loadFeatures(jsc);
        return mapNewsReport2Vector(jsc,newsRdd,featureMap);
    }

    private static void loadFeatures(JavaSparkContext jsc) {
        featureMap = FeatureUtil.loadFeatureMap(jsc, Parameters.hdfsHost+Parameters.featurePath);
    }

    public static JavaPairRDD<String, Vector> mapNewsReport2Vector(JavaSparkContext jsc, JavaRDD<NewsReport> newsRdd,
                                                              final Map<String, Feature> features) {

        if (features == null) {
            System.out.println("featureMap is null!");
        }
        //分词；输出 {word}_{docId}_{class} 1
        JavaPairRDD<String,Integer> docWordsRdd =  newsRdd.flatMapToPair(new PairFlatMapFunction<NewsReport, String, Integer>() {
            public Iterable<Tuple2<String, Integer>> call(NewsReport newsReport) throws Exception {
                List<Term> terms = Segment.segNewsreport(newsReport);
                List<Tuple2<String, Integer>> list = new ArrayList<Tuple2<String, Integer>>(terms.size());
                for (Term term : terms) {
                    //去除停用词
                    if (StopWords.isStopWord(term.getName())){
                        continue;
                    }
                    String key = term.getName() +"_"+newsReport.getCcnc_cat()+ "_" + newsReport.getId();
                    list.add(new Tuple2<String, Integer>(key, 1));
                }
                return list;
            }
        });

        JavaPairRDD<String,Integer> docTfRdd = docWordsRdd.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        JavaPairRDD<String, Feature> docFeature = docTfRdd.mapToPair(new PairFunction<Tuple2<String, Integer>, String, Feature>() {
            public Tuple2<String, Feature> call(Tuple2<String, Integer> tuple2) throws Exception {
                String word = tuple2._1.split("_", 2)[0];
                String cat_doc = tuple2._1.split("_", 2)[1];
                Feature feature = new Feature();
                feature.setWord(word);
                feature.setTf(tuple2._2);
                return new Tuple2<String, Feature>(cat_doc, feature);
            }
        });

        JavaPairRDD<String, Vector> vectorRdd = docFeature.groupByKey().mapToPair(new PairFunction<Tuple2<String,Iterable<Feature>>, String, Vector>() {
            public Tuple2<String, Vector> call(Tuple2<String, Iterable<Feature>> doc_feature) throws Exception {
                Iterator<Feature> iterator = doc_feature._2.iterator();
                TreeMap<Integer, Double> map = new TreeMap<Integer, Double>();
                while (iterator.hasNext()) {
                    //str format: word:tf
                    Feature feature = iterator.next();

                    if (!features.containsKey(feature.getWord())) {
                        continue;
                    }

                    int index = features.get(feature.getWord()).getIndex();
                    double tfidf = feature.getTf() * features.get(feature.getWord()).getIdf();
                    //map.put(index, tfidf);
                    // boolean model
                    map.put(index, 1.0);
                }

                int[] indices = new int[map.size()];
                double[] weights = new double[map.size()];
                int num = 0;
                for (Integer key : map.keySet()) {
                    indices[num] = key;
                    weights[num] = map.get(key);
                    num++;
                }
                Vector vector = Vectors.sparse(features.size(), indices, weights);
                return new Tuple2<String, Vector>(doc_feature._1, vector);
            }
        });

        return vectorRdd;
    }

    public static Vector mapNewsReport2Vector(NewsReport newsReport, Map<String,Integer> spaceMap,
                                              Map<String, Double> idfMap){
        List<Term> terms = Segment.segNewsreport(newsReport);
        Map<String, Integer> tfMap = new HashMap<String, Integer>();
        for (Term term : terms ) {
            String word = term.getName();
            //不在向量空间中
            if (!spaceMap.containsKey(word)){
                continue;
            }

            if (tfMap.containsKey(word)) {
                tfMap.put(word,tfMap.get(word)+1);
            }
            else {
                tfMap.put(word,1);
            }
        }

        TreeMap<Integer, Double> indexMap = new TreeMap<Integer, Double>();
        for (String word : tfMap.keySet()){
            indexMap.put(spaceMap.get(word),tfMap.get(word) * idfMap.get(word));
        }
        int[] indices = new int[indexMap.size()];
        double[] weights = new double[indexMap.size()];
        int num = 0;
        for (Integer key : indexMap.keySet()) {
            indices[num] = key;
            weights[num] = indexMap.get(key);
            num++;
        }
        Vector vector = Vectors.sparse(spaceMap.size(), indices, weights);
        return vector;
    }
}
