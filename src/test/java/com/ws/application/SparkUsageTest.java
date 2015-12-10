package com.ws.application;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.junit.Test;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * Created by Administrator on 2015/12/9.
 */
public class SparkUsageTest implements Serializable {

    public static void main ( String[] args ) {
        SparkConf conf = new SparkConf()
                .setAppName("testTopK")
                .setMaster("local");

        JavaSparkContext jsc = new JavaSparkContext(conf);

        List<String> list = Arrays.asList("wang_4.6","shuai_2.0","hello_1.6","world_5.6");

        JavaRDD<String> rdd = jsc.parallelize(list);

        List<Tuple2<Double,String>> miList = rdd.mapToPair(new PairFunction<String, Double, String>() {
            public Tuple2<Double, String> call(String s) throws Exception {
                System.out.println(s);
                String word = s.split("_")[0];
                Double mi = Double.parseDouble(s.split("_")[1]);
                return new Tuple2<Double, String>(mi, word);
            }
        }).sortByKey().top(2,new comp());

        for (Tuple2<Double,String> tuple2 : miList) {
            System.out.println(tuple2._2);
        }
    }

    private static class comp implements Comparator<Tuple2<Double,String>>,Serializable {

        public int compare(Tuple2<Double, String> o1, Tuple2<Double, String> o2) {
            return o1._1.compareTo(o2._1);
        }
    }
}
