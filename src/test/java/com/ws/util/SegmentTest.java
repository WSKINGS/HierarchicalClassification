package com.ws.util;

import org.ansj.domain.Term;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.*;

/**
 * Created by Administrator on 2015/11/16.
 */
public class SegmentTest {
    @Test
    public void testSegWord(){
        String text = "我爱吃苹果";

        List<Term> terms = Segment.segWords(text, Segment.SegType.SIMPLE);

        for (Term term : terms){
            System.out.print(term.getName()+" "+term.getNatureStr()+";");
        }
        assertThat(terms.size(), equalTo(4));

        terms = Segment.segWords(text, Segment.SegType.PRECISE);
        for (Term term : terms){
            System.out.print(term.getName()+" "+term.getNatureStr()+";");
        }
        assertThat(terms.size(), equalTo(4));
    }
}