package com.ws.process;

import com.ws.model.NewsReport;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Administrator on 2015/12/9.
 */
public class ClassCounter implements Serializable {

    public JavaPairRDD<String, Integer> countClassNum(final JavaRDD<NewsReport> newsRdd){
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
}
