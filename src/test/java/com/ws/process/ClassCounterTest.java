package com.ws.process;

import com.ws.application.TrainModel;
import com.ws.io.ContentProvider;
import com.ws.io.FileContentProvider;
import com.ws.model.InputRequest;
import com.ws.model.NewsReport;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.*;

/**
 * Created by Administrator on 2015/12/9.
 */
public class ClassCounterTest {
    private static final String filepath = "D:\\temp\\train.trs2.xml";

    @Test
    public void testClassCountNum() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        SparkConf conf = new SparkConf().setAppName("classification");
        conf.setMaster("local");

        JavaSparkContext jsc = new JavaSparkContext(conf);
        InputRequest request = new InputRequest();
        request.setJsc(jsc);
        request.setFilepath(filepath);

        //º”‘ÿ—µ¡∑ºØ
        ContentProvider contentProvider = new FileContentProvider();
        JavaRDD<NewsReport> src = contentProvider.getSource(request);
        ClassCounter classCounter = new ClassCounter();
        JavaPairRDD<String, Integer> classNumRdd = classCounter.countClassNum(src);

        Map<String,Integer> map = classNumRdd.collectAsMap();

        Map<String, List<String>> tree = null;
        Method[] methods = TrainModel.class.getDeclaredMethods();//("getHierarchicalMap");
        for (Method method : methods) {
            if (method.getName().equals("getHierarchicalMap")) {
                method.setAccessible(true);
                tree = (Map<String, List<String>>) method.invoke(new TrainModel(), map);
                break;
                //assertThat(tree.size(), equalTo(7));
            }
        }

        System.out.println(tree);
    }
}