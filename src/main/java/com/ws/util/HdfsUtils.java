package com.ws.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.classification.SVMModel;

import java.io.IOException;
import java.io.Serializable;

/**
 * Created by Administrator on 2015/12/28.
 */
public class HdfsUtils implements Serializable{
    private static final long serialVersionUID = 3404163232990735829L;

    public static boolean safeSave(JavaRDD rdd, String path){

        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);
            fs.delete(new Path(path), true);
            fs.close();

            rdd.saveAsTextFile(path);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return true;
    }

    public static boolean safeSaveModel(SVMModel model, SparkContext sc, String path) {
        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);
            fs.delete(new Path(path), true);
            fs.close();

            model.save(sc,path);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }
}
