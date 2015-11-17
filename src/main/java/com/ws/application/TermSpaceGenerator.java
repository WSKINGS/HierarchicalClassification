package com.ws.application;

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
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by Administrator on 2015/11/17.
 */
public class TermSpaceGenerator implements Serializable {

    private static final long serialVersionUID = -7750640499226974255L;

    public JavaPairRDD<String, Integer> generateTermSpace(JavaRDD<NewsReport> rdd){
        //分词
        JavaRDD<Term> termsRdd = rdd.flatMap(new FlatMapFunction<NewsReport, Term>() {

            public Iterable<Term> call(NewsReport newsReport) throws Exception {
                List<Term> terms = Segment.segWords(newsReport.getTitle(), Segment.SegType.SIMPLE);
                terms.addAll(Segment.segWords(newsReport.getContent(), Segment.SegType.SIMPLE));
//                List<String> terms = new ArrayList<String>();
                return terms;
            }
        });

        //去除停用词
        JavaRDD<Term> filteredRdd = termsRdd.filter(new Function<Term, Boolean>() {
            public Boolean call(Term term) throws Exception {
                return !StopWords.isStopWord(term.getName());
            }
        });

        //map
        JavaPairRDD<String, Integer> pairRDD = filteredRdd.mapToPair(new PairFunction<Term, String, Integer>() {
            public Tuple2<String, Integer> call(Term term) throws Exception {
                return new Tuple2<String, Integer>(term.getName(), 1);
            }
        });

        //reduce count
        JavaPairRDD<String, Integer> results = pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        return results;
    }

    public static void main ( String[] args ) {
        SparkConf conf = new SparkConf().setAppName("classification");
        if (args.length > 0 && args[0] != null) {
            conf.setMaster(args[0]);
        } else {
            conf.setMaster("local");
        }

        JavaSparkContext jsc = new JavaSparkContext(conf);

        ContentProvider contentProvider = new FileContentProvider();
        JavaRDD<NewsReport> src = contentProvider.getSource(jsc);

        TermSpaceGenerator termSpaceGenerator = new TermSpaceGenerator();
        JavaPairRDD<String, Integer> termSpave = termSpaceGenerator.generateTermSpace(src);

        //termSpaceGenerator.saveTermSpace(termSpave);


    }

    private void saveTermSpace(JavaPairRDD<String, Integer> termSpave) {
        Map<String, Integer> map = termSpave.collectAsMap();
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter("termSpace"));
            for (Map.Entry<String, Integer> entry : map.entrySet()){
                writer.write(entry.getKey()+"\t"+entry.getValue()+"\n");
            }
            writer.flush();
            writer.close();
        } catch (IOException e) {
            //do nothing
        }
    }
}
