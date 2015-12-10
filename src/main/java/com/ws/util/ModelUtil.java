package com.ws.util;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.classification.SVMModel;
import org.codehaus.janino.Java;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Administrator on 2015/12/10.
 */
public class ModelUtil implements Serializable {

    private static final long serialVersionUID = -3768044088667056955L;
    private static Map<String, SVMModel> modelMap = null;

    public static Map<String,SVMModel> getModelMap(JavaSparkContext jsc){
        if (modelMap == null) {
            loadModel(jsc);
        }
        return modelMap;
    }
    public static SVMModel getByName(JavaSparkContext jsc, String name){
        if (modelMap == null) {
            loadModel(jsc);
        }
        return modelMap.get(name);
    }

    private static void loadModel(JavaSparkContext jsc){
        modelMap = new HashMap<String, SVMModel>();
        Map<String,Iterable<String>> classMap = ClassHierarchicalUtil.loadClassHierarchical(jsc);
        for (String father:classMap.keySet()) {
            try {
                SVMModel model = SVMModel.load(jsc.sc(), Parameters.modelPath + father + ".model");
                modelMap.put(father,model);
            } catch (Exception e) {
                System.out.println("load "+father+".model error!");
            }
            for (String child : classMap.get(father)) {
                try {
                    SVMModel model = SVMModel.load(jsc.sc(), Parameters.modelPath+child+".model");
                    modelMap.put(child,model);
                } catch (Exception e) {
                    System.out.println("load "+child+".model error!");
                }
            }

        }
    }
}
