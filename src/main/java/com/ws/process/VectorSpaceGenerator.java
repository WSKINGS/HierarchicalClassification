package com.ws.process;

import com.google.common.collect.Iterables;
import com.ws.model.Feature;
import com.ws.model.NewsReport;
import com.ws.util.Parameters;
import com.ws.util.Segment;
import com.ws.util.StopWords;
import org.ansj.domain.Term;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

/**
 * Created by Administrator on 2015/12/7.
 */
public class VectorSpaceGenerator implements Serializable {
    private static final long serialVersionUID = 246261014066382025L;
    private static Logger logger = Logger.getLogger(VectorSpaceGenerator.class);

    public List<Feature> generateVectorSpace(final JavaRDD<NewsReport> newsRdd,JavaPairRDD<String, Integer> classCountRdd){
        final long total = newsRdd.count();

        //分词；输出 {word}_{docId} {class}
        JavaPairRDD<String,String> docWordsRdd =  newsRdd.flatMapToPair(new PairFlatMapFunction<NewsReport, String, String>() {
            public Iterable<Tuple2<String, String>> call(NewsReport newsReport) throws Exception {
                List<Term> terms = Segment.segNewsreport(newsReport);
                List<Tuple2<String, String>> list = new ArrayList<Tuple2<String, String>>(terms.size());
                for (Term term : terms) {
                    //去除停用词
                    if (StopWords.isStopWord(term.getName())){
                        //logger.info(String.format("find stop word: %s in news %s", term.getName(), newsReport.getId()));
                        continue;
                    }
                    String key = term.getName() + "_" + newsReport.getId();
                    list.add(new Tuple2<String, String>(key, newsReport.getCcnc_cat()));
                }
                return list;
            }
        });

        JavaPairRDD<String,String> distinctWordRdd = docWordsRdd.distinct();

        final JavaPairRDD<String,String> word_catDoc = distinctWordRdd.mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {

            public Tuple2<String, String> call(Tuple2<String, String> word_doc_cat) throws Exception {
                String[] arr = word_doc_cat._1.split("_");
                String word = arr[0];
                String cat_doc = word_doc_cat._2 + "_" + arr[1];
                return new Tuple2<String, String>(word, cat_doc);
            }
        });

        //根据df过滤后的倒排列表
        JavaPairRDD<String, Iterable<String>> wordGroup = word_catDoc.groupByKey()
                .filter(new Function<Tuple2<String, Iterable<String>>, Boolean>() {
                    public Boolean call(Tuple2<String, Iterable<String>> word_docList) throws Exception {
                        int size = Iterables.size(word_docList._2);
                        if (size < Parameters.dfThreshold){
                            //logger.info(String.format("filter word: %s by df", word_docList._1));
                            return false;
                        }
                        return true;
                    }
                });

        JavaPairRDD<Tuple2<String,Iterable<String>>,Tuple2<String, Integer>> cartesianRdd = wordGroup.cartesian(classCountRdd);

        JavaPairRDD<String,Double> miRdd = cartesianRdd.mapToPair(new PairFunction<Tuple2<Tuple2<String, Iterable<String>>, Tuple2<String, Integer>>, String, Double>() {
            public Tuple2<String, Double> call(Tuple2<Tuple2<String, Iterable<String>>, Tuple2<String, Integer>> tuple2Tuple2Tuple2) throws Exception {
                Tuple2<String, Iterable<String>> word_list = tuple2Tuple2Tuple2._1;
                Tuple2<String, Integer> class_num = tuple2Tuple2Tuple2._2;
//                String word_class = word_list._1+"_"+class_num._1;
                String word = word_list._1;
                int trueClass = 0;
                int df = 0;
                for (String cat_doc : word_list._2) {
                    String cat = cat_doc.split("_")[0];
                    if (cat.equals(class_num._1)) {
                        trueClass++;
                    }
                    df++;
                }
                double mi = Math.log((trueClass*total+1.0)*1.0/(df*class_num._2+1.0));
               return new Tuple2<String,Double>(word,mi);
            }
        });

        // 1.去除重复单词，保留每个单词最大的mi值
        // 2.选择Top N
        List<Tuple2<Double,String>> topN_mi = miRdd.reduceByKey(new Function2<Double, Double, Double>() {
            public Double call(Double mi1, Double mi2) throws Exception {
                return mi1 >= mi2 ? mi1 : mi2;
            }
        }).mapToPair(new PairFunction<Tuple2<String, Double>, Double, String>() {
            public Tuple2<Double, String> call(Tuple2<String, Double> word_mi) throws Exception {
                return new Tuple2<Double, String>(word_mi._2, word_mi._1);
            }
        }).sortByKey().top(Parameters.TopN, new T_comp());

        final Map<String,Boolean> spaceMap = new HashMap<String,Boolean>();
        for (Tuple2<Double, String> tuple2 : topN_mi) {
            if (!spaceMap.containsKey(tuple2._2)){
                spaceMap.put(tuple2._2,true);
            }
        }

        //计算向量空间中特征的idf值
        JavaRDD<Feature> featureRdd = wordGroup.filter(new Function<Tuple2<String, Iterable<String>>, Boolean>() {
            public Boolean call(Tuple2<String, Iterable<String>> word_docList) throws Exception {
                if (spaceMap.containsKey(word_docList._1)) {
                    return true;
                }
                return false;
            }
        }).map(new Function<Tuple2<String, Iterable<String>>, Feature>() {
            public Feature call(Tuple2<String, Iterable<String>> word_docList) throws Exception {
                long docNum = Iterables.size(word_docList._2);
                double idf = Math.log((total) / (docNum));
                Feature feature = new Feature();
                feature.setIdf(idf);
                feature.setWord(word_docList._1);
                feature.setTf((int) docNum);
                return feature;
            }
        });

        //Map<String, Feature> featureMap = new HashMap<String, Feature>();
        List<Feature> features = featureRdd.collect();
        for (int i=0; i<features.size(); i++) {
            Feature feature = features.get(i);
            feature.setIndex(i);
            //featureMap.put(feature.getWord(),feature);
        }
        return features;
    }

    private class T_comp implements Comparator<Tuple2<Double, String>>,Serializable {

        public int compare(Tuple2<Double, String> o1, Tuple2<Double, String> o2) {
            return o1._1.compareTo(o2._1);
        }
    }
}

