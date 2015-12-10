package com.ws.util;

import java.io.Serializable;

/**
 * Created by Administrator on 2015/12/10.
 */
public class Parameters implements Serializable {
    public static final String filepath="hdfs://10.1.0.149:9000/user/wangshuai/train.json";
    //    public static final String filepath = "D:\\temp\\train.trs2.xml";

    public static final String testPath="hdfs://10.1.0.149:9000/user/wangshuai/test.json";
    public static final String testResult="hdfs://10.1.0.149:9000/user/wangshuai/test/result";

//    public static final String modelPath="hdfs://10.1.0.149:9000/user/wangshuai/model/";
    public static final String modelPath="hdfs://10.1.0.149:9000/user/wangshuai/test/model/";

    //public static final String featurePath="hdfs://10.1.0.149:9000/user/wangshuai/features";
    public static final String featurePath="hdfs://10.1.0.149:9000/user/wangshuai/test/features";

    public static final String classPath = "hdfs://10.1.0.149:9000/user/wangshuai/test/classCount";

    public static final int dfThreshold = 2;
    public static final double miThreshold = 9;
    public static final int TopN = 200;
}
