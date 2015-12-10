package com.ws.classifier;

import com.ws.model.NewsReport;
import com.ws.util.ClassHierarchicalUtils;
import com.ws.util.ModelUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Map;

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

    public double predict(SVMModel model, Vector vector){
        if (model == null) {
            return 0.0;
        }
        return model.predict(vector);
    }

    public JavaPairRDD<String,String> predict(final JavaSparkContext jsc, JavaRDD<NewsReport> src){
        JavaPairRDD<String, Vector> docVectorRdd = NewsReportTransformation.mapNewsReport2Vector(jsc, src);
        final Map<String, Iterable<String>> hierarchical = ClassHierarchicalUtils.loadClassHierarchical(jsc);

        final Map<String, SVMModel> modelMap = ModelUtil.getModelMap(jsc);

        JavaPairRDD<String, String> docTypeRdd = docVectorRdd.mapToPair(new PairFunction<Tuple2<String, Vector>, String, String>() {
            public Tuple2<String, String> call(Tuple2<String, Vector> stringVectorTuple2) throws Exception {
                String type = "type";
                for (String father : hierarchical.keySet()) {
                    SVMModel model = modelMap.get(father);
                    double score = predict(model,stringVectorTuple2._2);
                    if (score == 1.0) {
                        for (String child : hierarchical.get(father)) {
                            SVMModel childModel = modelMap.get(child);
                            score = childModel.predict(stringVectorTuple2._2);
                            if (score == 1.0) {
                                type += "_" + child;
                            }
                        }
                    }
                }
                return new Tuple2<String, String>(stringVectorTuple2._1, type);
            }
        });

        return docTypeRdd;
    }
}
