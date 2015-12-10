package com.ws.util;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Created by Administrator on 2015/12/10.
 */
public class ClassHierarchicalUtil implements Serializable{

    private static final long serialVersionUID = -1377817106223751555L;
    private static Map<String, Iterable<String>> hierarchical = null;
    public static Map<String, Iterable<String>> loadClassHierarchical(JavaSparkContext jsc) {
        if (hierarchical != null) {
            return hierarchical;
        } else {
            return loadClassHierarchicalFromPath(jsc);
        }
    }

    public static Map<String, Iterable<String>> loadClassHierarchicalFromPath(JavaSparkContext jsc){
        JavaRDD<String> rdd = jsc.textFile(Parameters.classPath);
        JavaPairRDD<String, Iterable<String>> result = rdd.mapToPair(new PairFunction<String, String, String>() {
            public Tuple2<String, String> call(String s) throws Exception {
                s = s.substring(1, s.length() - 1);
                String label = s.split(",")[0];
                if (!label.contains(".")) {
                    return new Tuple2<String, String>(label, "null");
                }
                String father = label.split("[.]")[0];
                return new Tuple2<String, String>(father, label);
            }
        }).filter(new Function<Tuple2<String, String>, Boolean>() {
            public Boolean call(Tuple2<String, String> father_child) throws Exception {
                if (father_child._2.equals("null")) {
                    return false;
                }
                return true;
            }
        }).groupByKey();
        return result.collectAsMap();
    }
}
