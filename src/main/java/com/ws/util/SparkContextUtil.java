package com.ws.util;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by Administrator on 2015/12/15.
 */
public class SparkContextUtil {
    public static JavaSparkContext getSparkContext(String name){
        return getSparkContext(Parameters.masterUrl);
    }
    public static JavaSparkContext getSparkContext(String name,String master){
        SparkConf conf = new SparkConf().setAppName(name);
        conf.setMaster(master);
        conf.set("spark.executor.memory","7G")
                .set("spark.driver..memory","7G");

        JavaSparkContext jsc = new JavaSparkContext(conf);
        return jsc;
    }
}
