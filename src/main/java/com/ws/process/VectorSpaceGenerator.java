package com.ws.process;

import com.google.common.collect.Iterables;
import com.ws.model.Feature;
import com.ws.model.NewsReport;
import com.ws.util.Segment;
import com.ws.util.StopWords;
import org.ansj.domain.Term;
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
    private static final int dfThreshold = 2;
    private static final double miThreshold = 0.3;

    public List<Feature> generateVectorSpace(final JavaRDD<NewsReport> newsRdd){
        final long total = newsRdd.count();

        //分词；输出 {word}_{docId} {class}
        JavaPairRDD<String,String> docWordsRdd =  newsRdd.flatMapToPair(new PairFlatMapFunction<NewsReport, String, String>() {
            public Iterable<Tuple2<String, String>> call(NewsReport newsReport) throws Exception {
                List<Term> terms = Segment.segNewsreport(newsReport);
                List<Tuple2<String, String>> list = new ArrayList<Tuple2<String, String>>(terms.size());
                for (Term term : terms) {
                    //去除停用词
                    if (StopWords.isStopWord(term.getName())){
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
                        if (size < dfThreshold){
                            return false;
                        }
                        return true;
                    }
                });

        //统计每个类别的数目
        JavaPairRDD<String, Integer> classCount = countClassNum(newsRdd);

        JavaPairRDD<Tuple2<String,Iterable<String>>,Tuple2<String, Integer>> cartesianRdd = wordGroup.cartesian(classCount);

        JavaPairRDD<Double,String> miRdd = cartesianRdd.mapToPair(new PairFunction<Tuple2<Tuple2<String, Iterable<String>>, Tuple2<String, Integer>>, Double, String>() {
            public Tuple2<Double, String> call(Tuple2<Tuple2<String, Iterable<String>>, Tuple2<String, Integer>> tuple2Tuple2Tuple2) throws Exception {
                Tuple2<String, Iterable<String>> word_list = tuple2Tuple2Tuple2._1;
                Tuple2<String, Integer> class_num = tuple2Tuple2Tuple2._2;
                String word_class = word_list._1+"_"+class_num._1;
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
               return new Tuple2<Double,String>(mi, word_class);
            }
        });

        //逆转miRDD,实现topk
        JavaPairRDD<Double, String> filteredMi = miRdd.filter(new Function<Tuple2<Double, String>, Boolean>() {
            public Boolean call(Tuple2<Double, String> doubleStringTuple2) throws Exception {
                return doubleStringTuple2._1 >= miThreshold;
            }
        });

        JavaRDD<String> spaceWordRdd = filteredMi.map(new Function<Tuple2<Double, String>, String>() {
            public String call(Tuple2<Double, String> doubleStringTuple2) throws Exception {
                return doubleStringTuple2._2.split("_")[0];
            }
        }).distinct();

        List<String> space = spaceWordRdd.collect();
        final Map<String,Boolean> spaceMap = new HashMap<String,Boolean>();
        for (String word : space) {
            if (!spaceMap.containsKey(word)){
                spaceMap.put(word,true);
            }
        }

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
                double idf = Math.log((total + 1.0) / (docNum + 1.0));
                Feature feature = new Feature();
                feature.setIdf(idf);
                feature.setWord(word_docList._1);
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

    private JavaPairRDD<String, Integer> countClassNum(final JavaRDD<NewsReport> newsRdd){
        JavaPairRDD<String,Integer> classRDD = newsRdd.flatMapToPair(new PairFlatMapFunction<NewsReport, String, Integer>() {
            public Iterable<Tuple2<String, Integer>> call(NewsReport newsReport) throws Exception {
                List<Tuple2<String, Integer>> list = new ArrayList<Tuple2<String, Integer>>(2);
                String cat = newsReport.getCcnc_cat();
                if (cat == null || !cat.contains(".")){
                    System.out.println("log: newsreport is null!"+newsReport.getTitle());
                    return list;
                }
                list.add(new Tuple2<String, Integer>(cat, 1));
                list.add(new Tuple2<String, Integer>(cat.split("[.]")[0], 1));
                return list;
            }
        });

        return classRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer+integer2;
            }
        });
    }

    private class T_comp implements Comparator<Tuple2<Double, String>>,Serializable {

        public int compare(Tuple2<Double, String> o1, Tuple2<Double, String> o2) {
            return (int) (o1._1-o2._1);
        }
    }
}

