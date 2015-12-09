package com.ws.classifier;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.regression.LabeledPoint;

import java.io.Serializable;

/**
 * Created by Administrator on 2015/11/17.
 */
public class SvmClassifier implements Serializable {

    private static final long serialVersionUID = -3970784185151903345L;

    public SVMModel train(JavaRDD<LabeledPoint> trainData, int numIterations){
        return SVMWithSGD.train(trainData.rdd(), numIterations);
    }

    public double predict(SVMModel model, LabeledPoint point){

        return model.predict(point.features());
    }
}
