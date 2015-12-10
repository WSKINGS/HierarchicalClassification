package com.ws.assistant;

import com.ws.io.ContentProvider;
import com.ws.io.FileContentProvider;
import com.ws.model.InputRequest;
import com.ws.model.NewsReport;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

/**
 * Created by Administrator on 2015/11/15.
 */
public class StatisticData {
    public static void main(String[] args) throws IOException {
        SparkConf conf = new SparkConf().setAppName("classification");
        if (args.length > 0 && args[0] != null) {
            conf.setMaster(args[0]);
        } else {
            conf.setMaster("local");
        }

        JavaSparkContext jsc = new JavaSparkContext(conf);
        InputRequest request = new InputRequest();
        request.setJsc(jsc);

        ContentProvider contentProvider = new FileContentProvider();
        JavaRDD<NewsReport> src = contentProvider.getSource(request);

        JavaPairRDD<String, Integer> fatherRDD = src.mapToPair(new PairFunction<NewsReport, String, Integer>() {
            public Tuple2<String, Integer> call(NewsReport newsReport) throws Exception {
                String id = newsReport.getCcnc_cat();
                String key = id.split("．")[0];
                return new Tuple2<String, Integer>(key,1);
            }
        });

        JavaPairRDD<String, Integer> result = fatherRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = 6358312953775480543L;

            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        Map<String, Integer> map = result.collectAsMap();
        final BufferedWriter writer = new BufferedWriter(new FileWriter("father.csv"));
        for (Map.Entry entry : map.entrySet()){
            writer.write(entry.getKey()+","+entry.getValue()+"\n");
        }

        writer.flush();
        writer.close();

        JavaPairRDD<String,Integer> childRDD = src.mapToPair(new PairFunction<NewsReport, String, Integer>() {
            private static final long serialVersionUID = -2259960540778710862L;

            public Tuple2<String, Integer> call(NewsReport newsReport) throws Exception {
                String id = newsReport.getCcnc_cat();
                return new Tuple2<String, Integer>(id, 1);
            }
        });

        JavaPairRDD<String, Integer> result2 = childRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = 6358312953775480543L;

            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        Map<String, Integer> childMap = result2.collectAsMap();
        final BufferedWriter writer2 = new BufferedWriter(new FileWriter("children.csv"));
        for (Map.Entry entry : childMap.entrySet()){
            writer2.write(entry.getKey()+","+entry.getValue()+"\n");
        }

        writer2.flush();
        writer2.close();

    }
}
