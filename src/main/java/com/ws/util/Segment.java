package com.ws.util;

import com.ws.model.NewsReport;
import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.BaseAnalysis;
import org.ansj.splitWord.analysis.NlpAnalysis;
import org.ansj.splitWord.analysis.ToAnalysis;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Administrator on 2015/11/16.
 */
public class Segment implements Serializable {
    private static final long serialVersionUID = 2782772945908197954L;

    public static List<Term> segWords(String text, SegType type){
        switch (type){
            case SIMPLE:
                return BaseAnalysis.parse(text);
            case PRECISE:
                return ToAnalysis.parse(text);
            case NLP:
                return NlpAnalysis.parse(text);
            default:
                return new ArrayList<Term>();
        }
    }

    public static List<Term> segNewsreport(NewsReport report){
        List<Term> terms = segWords(report.getTitle(),SegType.SIMPLE);
        terms.addAll(segWords(report.getContent(), SegType.SIMPLE));
        return terms;
    }

    public enum SegType{
        SIMPLE,PRECISE,NLP
    }
}