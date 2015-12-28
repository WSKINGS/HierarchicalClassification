package com.ws.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.classification.SVMModel;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;

/**
 * Created by Administrator on 2015/12/28.
 */
public class HdfsUtils implements Serializable{
    private static final long serialVersionUID = 3404163232990735829L;

    public static boolean safeSave(JavaRDD rdd, String path){
        remove(path);
        rdd.saveAsTextFile(Parameters.hdfsHost+path);

        return true;
    }

    public static boolean safeSave(JavaPairRDD rdd, String path){
        remove(path);
        rdd.saveAsTextFile(Parameters.hdfsHost+path);

        return true;
    }

    public static boolean safeSaveModel(SVMModel model, SparkContext sc, String path) {
        remove(path);
        model.save(sc,Parameters.hdfsHost+path);
        return true;
    }

    private static boolean remove(String path){
        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(URI.create(Parameters.hdfsHost),conf);
            fs.delete(new Path(path), true);
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }
}
