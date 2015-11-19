package com.ws.classifier;

import com.ws.model.NewsReport;
import com.ws.util.Segment;
import org.ansj.domain.Term;
import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import scala.Int;

import java.io.Serializable;
import java.util.*;

/**
 * Created by Administrator on 2015/11/18.
 */
public class NewsReportTransformation implements Serializable {
    private static final long serialVersionUID = 2255395655953170782L;

    public static Vector changeNewsReport2Vector(Map<String, Integer> wordIndexMap, NewsReport newsReport){
        List<Term> terms = Segment.segNewsreport(newsReport);
        Set<Integer> set = new HashSet<Integer>(terms.size());
        for (Term term : terms) {
            if (!wordIndexMap.containsKey(term.getName())) {
                System.out.println("not found : "+term.getName());
                continue;
            }
            set.add(wordIndexMap.get(term.getName()));
        }

        Integer[] temp = new Integer[set.size()];
        set.toArray(temp);
        int[] indexes = ArrayUtils.toPrimitive(temp);
        Arrays.sort(indexes);

        double[] data = new double[indexes.length];
        Arrays.fill(data, 1.0);
        Vector vector = Vectors.sparse(wordIndexMap.size(), indexes, data);
        return vector;
    }
}
