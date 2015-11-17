package com.ws.util;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by Administrator on 2015/11/17.
 */
public class StopWordsTest {
    @Test
    public void testIsStopWord(){
        String word = "的";
        String word2 = "王帅";
        assertTrue(StopWords.isStopWord(word));
        assertFalse(StopWords.isStopWord(word2));
    }
}