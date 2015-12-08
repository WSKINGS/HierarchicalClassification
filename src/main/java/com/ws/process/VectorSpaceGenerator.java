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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by Administrator on 2015/12/7.
 */
public class VectorSpaceGenerator implements Serializable {
    private static final long serialVersionUID = 246261014066382025L;
    private static final int dfThreshold = 2;

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
                    list.add(new Tuple2<String, String>(key, newsReport.getCatId()));
                }
                return list;
            }
        });

        JavaPairRDD<String,String> distinctWordRdd = docWordsRdd.distinct();

        JavaPairRDD<String,String> word_catDoc = distinctWordRdd.mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {

            public Tuple2<String, String> call(Tuple2<String, String> word_doc_cat) throws Exception {
                String[] arr = word_doc_cat._1.split("_");
                String word = arr[0];
                String cat_doc = word_doc_cat._2 + "_" + arr[1];
                return new Tuple2<String, String>(word, cat_doc);
            }
        });

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

        JavaPairRDD<String,Double> miRdd = cartesianRdd.mapToPair(new PairFunction<Tuple2<Tuple2<String, Iterable<String>>, Tuple2<String, Integer>>, String, Double>() {
            public Tuple2<String, Double> call(Tuple2<Tuple2<String, Iterable<String>>, Tuple2<String, Integer>> tuple2Tuple2Tuple2) throws Exception {
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
                double mi = Math.log((trueClass*total)*1.0/(df*class_num._2));
               return new Tuple2<String, Double>(word_class,mi);
            }
        });

        return null;
    }

    private JavaPairRDD<String, Integer> countClassNum(final JavaRDD<NewsReport> newsRdd){
        JavaPairRDD<String,Integer> classRDD = newsRdd.flatMapToPair(new PairFlatMapFunction<NewsReport, String, Integer>() {
            public Iterable<Tuple2<String, Integer>> call(NewsReport newsReport) throws Exception {
                List<Tuple2<String, Integer>> list = new ArrayList<Tuple2<String, Integer>>(2);
                String cat = newsReport.getCatId();
                list.add(new Tuple2<String, Integer>(cat, 1));
                list.add(new Tuple2<String, Integer>(cat.split(".")[0], 1));
                return list;
            }
        });

        return classRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer+integer2;
            }
        });
    }
}
