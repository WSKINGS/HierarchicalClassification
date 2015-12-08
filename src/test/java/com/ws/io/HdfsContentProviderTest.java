package com.ws.io;

import com.ws.model.InputRequest;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by Administrator on 2015/11/25.
 */
public class HdfsContentProviderTest {

    @Test
    public void testGetSource() throws Exception {
        SparkConf conf = new SparkConf().setAppName("testHdfsContentProvider");
        conf.setMaster("spark://10.1.0.149:7077");

        InputRequest request = new InputRequest();
        JavaSparkContext jsc = new JavaSparkContext(conf);
        request.setJsc(jsc);
        HdfsContentProvider contentProvider = new HdfsContentProvider();
        contentProvider.getSource(request);
    }
}