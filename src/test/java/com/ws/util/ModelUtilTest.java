package com.ws.util;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.classification.SVMModel;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;

/**
 * Created by Administrator on 2015/12/10.
 */
public class ModelUtilTest {

    @Test
    public void testGetModelMap() throws Exception {
        SparkConf conf = new SparkConf()
                .setAppName("classification")
                .set("spark.executor.memory","6g")
                .set("spark.driver.memory","4g");
            conf.setMaster("local");

        JavaSparkContext jsc = new JavaSparkContext(conf);

        Map<String, SVMModel> modelMap = ModelUtil.getModelMap(jsc);
        System.out.println(modelMap.size());
    }
}