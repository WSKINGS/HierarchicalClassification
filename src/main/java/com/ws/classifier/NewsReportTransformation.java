package com.ws.classifier;

import com.ws.model.NewsReport;
import com.ws.util.Segment;
import org.ansj.domain.Term;
import org.apache.spark.api.java.JavaPairRDD;
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

    public static JavaPairRDD<String, Vector> mapWords2Vector(JavaPairRDD<String, Integer> wordsOfNews,
                                                              final Map<String, Integer> spaceMap,
                                                              final Map<String, Double> idfMap) {
        JavaPairRDD<String, String> docWordRDD = wordsOfNews.mapToPair(new PairFunction<Tuple2<String, Integer>, String, String>() {
            public Tuple2<String, String> call(Tuple2<String, Integer> tuple2) throws Exception {
                int index = tuple2._1.lastIndexOf("_");
                String key = tuple2._1.substring(0, index);
                String value = tuple2._1.substring(index + 1) + ":" + tuple2._2;
                return new Tuple2<String, String>(key, value);
            }
        });

        JavaPairRDD<String, Iterable<String>> docRdd = docWordRDD.groupByKey();

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
                    double tfidf = tf * idfMap.get(word);
                    map.put(index, tfidf);
                }

                int[] indices = new int[map.size()];
                double[] weights = new double[map.size()];
                int num = 0;
                for (Integer key : map.keySet()) {
                    indices[num] = key;
                    weights[num] = map.get(key);
                    num++;
                }
                Vector vector = Vectors.sparse(spaceMap.size(), indices, weights);
                return new Tuple2<String, Vector>(stringIterableTuple2._1, vector);
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
