package com.ws.application;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.io.Serializable;

/**
 * Created by Administrator on 2015/12/5.
 */
public class StreamingClassifier implements Serializable {
    private static final long serialVersionUID = 1304756351749706747L;

    public static void main ( String[] args ) {
        SparkConf sparkConf = new SparkConf().setAppName("Streaming Classification");
        sparkConf.setMaster("local");

        JavaStreamingContext jsc = new JavaStreamingContext(sparkConf, Durations.minutes(30));

        jsc.socketTextStream("",90);

    }
}
