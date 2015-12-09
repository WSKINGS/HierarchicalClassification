package com.ws.model;

import java.io.Serializable;

/**
 * Created by Administrator on 2015/12/7.
 */
public class Feature implements Serializable {
    private static final long serialVersionUID = -909304887002878673L;

    private int index;
    private String word;
    private double idf;
    private int tf;

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public double getIdf() {
        return idf;
    }

    public void setIdf(double idf) {
        this.idf = idf;
    }

    public int getTf() {
        return tf;
    }

    public void setTf(int tf) {
        this.tf = tf;
    }
}
