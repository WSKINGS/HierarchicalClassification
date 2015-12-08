package com.ws.util;

import com.google.gson.Gson;
import com.ws.model.NewsReport;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by Administrator on 2015/12/7.
 */
public class NewsReportUtilTest {

    @Test
    public void testFromJson(){
        Gson gson = new Gson();
        gson.fromJson("{", NewsReport.class);
    }

}