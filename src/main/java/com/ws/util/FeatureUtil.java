package com.ws.util;

import com.google.gson.Gson;
import com.ws.model.Feature;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Created by Administrator on 2015/12/9.
 */
public class FeatureUtil implements Serializable {

    public static Map<String, Feature> loadFeatureMap(JavaSparkContext jsc, String path){
        JavaRDD<String> rdd = jsc.textFile(path);
        JavaPairRDD<String,Feature> featureRDD = rdd.mapToPair(new PairFunction<String, String, Feature>() {
            public Tuple2<String, Feature> call(String s) throws Exception {
                Gson gson = new Gson();
                Feature feature = gson.fromJson(s, Feature.class);
                return new Tuple2<String, Feature>(feature.getWord(), feature);
            }
        });

        return featureRDD.collectAsMap();
    }

    public static Map<String,Feature> saveFeatures(JavaSparkContext jsc, List<Feature> featureList){
        JavaRDD<Feature> featureRdd = jsc.parallelize(featureList);
        featureRdd.saveAsTextFile(Parameters.featurePath);

        JavaPairRDD<String, Feature> featurePair = featureRdd.mapToPair(new PairFunction<Feature, String, Feature>() {
            public Tuple2<String, Feature> call(Feature feature) throws Exception {
                return new Tuple2<String, Feature>(feature.getWord(), feature);
            }
        });

        return featurePair.collectAsMap();
    }
}
