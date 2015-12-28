package com.ws.util;

import java.io.Serializable;

/**
 * Created by Administrator on 2015/12/10.
 */
public class Parameters implements Serializable {
    public static final String hdfsHost = "hdfs://10.1.0.149:9000";
    public static final String filepath="/user/wangshuai/train.json";
//        public static final String filepath = "D:\\temp\\train.trs2.xml";

    public static final String testPath="/user/wangshuai/test.json";
    public static final String testResult="/user/wangshuai/test/result";

//    public static final String modelPath="/user/wangshuai/model/";
    public static final String modelPath="/user/wangshuai/test/model/";

    //public static final String featurePath="/user/wangshuai/features";
    public static final String featurePath="/user/wangshuai/test/features";

    public static final String classPath = "/user/wangshuai/test/classCount";

    //public static final String stopWords = "/home/wangshuai/stopwords.txt";
    public static final String stopWords = "stopwords.txt";

    public static final int dfThreshold = 2;
    public static final double miThreshold = 9;
    public static final int TopN = 500;


    private static final long serialVersionUID = -3767412376778361271L;
    public static final String masterUrl="local";
//    public static final String masterUrl="spark://10.1.0.149:7077";
}
