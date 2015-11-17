package com.ws.application;

import com.ws.io.ContentProvider;
import com.ws.io.FileContentProvider;
import com.ws.model.ClusterNode;
import com.ws.model.NewsReport;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.linalg.Matrices;
import org.apache.spark.mllib.linalg.Matrix;
import org.codehaus.janino.Java;
import scala.Int;
import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Created by Administrator on 2015/10/27.
 */
public class TestApplication implements Serializable {

    private static final long serialVersionUID = -6327540086331827844L;

    public static void main(String[] args) throws IOException {
        SparkConf conf = new SparkConf().setAppName("classification");
        if (args.length > 0 && args[0] != null) {
            conf.setMaster(args[0]);
        } else {
            conf.setMaster("local");
        }

        JavaSparkContext jsc = new JavaSparkContext(conf);

        ContentProvider contentProvider = new FileContentProvider();
        JavaRDD<NewsReport> src = contentProvider.getSource(jsc);

        JavaPairRDD<String, Integer> fatherRDD = src.mapToPair(new PairFunction<NewsReport, String, Integer>() {
            public Tuple2<String, Integer> call(NewsReport newsReport) throws Exception {
                String id = newsReport.getCatId();
                String key = id.split("．")[0];
                return new Tuple2<String, Integer>(key,1);
            }
        });

        JavaPairRDD<String, Integer> result = fatherRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = 6358312953775480543L;

            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        Map<String, Integer> map = result.collectAsMap();
        final BufferedWriter writer = new BufferedWriter(new FileWriter("father.csv"));
        for (Map.Entry entry : map.entrySet()){
            writer.write(entry.getKey()+","+entry.getValue()+"\n");
        }
//        result.foreach(new VoidFunction<Tuple2<String, Integer>>() {
//            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
//                writer.write(stringIntegerTuple2._1+","+stringIntegerTuple2._2);
//            }
//        });

        writer.flush();
        writer.close();

        JavaPairRDD<String,Integer> childRDD = src.mapToPair(new PairFunction<NewsReport, String, Integer>() {
            private static final long serialVersionUID = -2259960540778710862L;

            public Tuple2<String, Integer> call(NewsReport newsReport) throws Exception {
                String id = newsReport.getCatId();
                return new Tuple2<String, Integer>(id, 1);
            }
        });

        JavaPairRDD<String, Integer> result2 = childRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = 6358312953775480543L;

            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        Map<String, Integer> childMap = result2.collectAsMap();
        final BufferedWriter writer2 = new BufferedWriter(new FileWriter("children.csv"));
        for (Map.Entry entry : childMap.entrySet()){
            writer2.write(entry.getKey()+","+entry.getValue()+"\n");
        }
//        result.foreach(new VoidFunction<Tuple2<String, Integer>>() {
//            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
//                writer.write(stringIntegerTuple2._1+","+stringIntegerTuple2._2);
//            }
//        });

        writer2.flush();
        writer2.close();

       // JavaRDD<ClusterNode> vectorRdd = mapArticleToVector(src);

        //JavaPairRDD<ClusterNode, ClusterNode> pairRDD = vectorRdd.cartesian(vectorRdd);

        //ClusterNode node = getNearestClusterNode(pairRDD);

       // Matrix matrix = Matrices.sparse(3,2,new int[]{0,1,3}, new int[]{0,2,1}, new double[] {9,6,8});

        //if (node == null){
           // break;
        //}


    }

    private static JavaRDD<ClusterNode> mapArticleToVector(JavaRDD<NewsReport> src) {
        return src.map(new Function<NewsReport, ClusterNode>() {
            public ClusterNode call(NewsReport newsReport) throws Exception {
                ClusterNode node = new ClusterNode();
                //todo
                return node;
            }
        });
    }

    public static ClusterNode getNearestClusterNode(JavaPairRDD<ClusterNode, ClusterNode> pairRDD) {
//        JavaRDD<ClusterNode> rdd = pairRDD.map(new Function<Tuple2<ClusterNode, ClusterNode>, ClusterNode>() {
//            public ClusterNode call(Tuple2<ClusterNode, ClusterNode> tuple) throws Exception {
//                ClusterNode node = mergeNode(tuple._1, tuple._2);
//                return node;
//            }
//
//            private ClusterNode mergeNode(ClusterNode clusterNode, ClusterNode clusterNode1) {
//                ClusterNode node = new ClusterNode();
//                //
//                return node;
//            }
//        });
        JavaPairRDD<Double,ClusterNode> rdd = pairRDD.mapToPair(new PairFunction<Tuple2<ClusterNode, ClusterNode>, Double, ClusterNode>() {
            public Tuple2<Double, ClusterNode> call(Tuple2<ClusterNode, ClusterNode> tuple2) throws Exception {
                return null;
            }
        });

        List<Tuple2<Double,ClusterNode>> top = rdd.top(1, new Comparator<Tuple2<Double, ClusterNode>>() {
            public int compare(Tuple2<Double, ClusterNode> o1, Tuple2<Double, ClusterNode> o2) {
                return o2._1 > o1._1 ? 1 : (o2._1 < o1._1 ? -1 : 0);
            }
        });
        return top.get(0)._2;
    }
}
