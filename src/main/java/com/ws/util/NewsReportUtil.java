package com.ws.util;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.ws.model.NewsReport;

import java.io.Serializable;

/**
 * Created by Administrator on 2015/11/26.
 */
public class NewsReportUtil implements Serializable {
    private static final long serialVersionUID = 7077392334782831728L;
    private static JsonParser parser = new JsonParser();

    public static NewsReport fromJsonToNewsReport(String jsonStr) {
        return fromJsonToNewsReport(jsonStr, true);
    }
    public static NewsReport fromJsonToNewsReport(String jsonStr, boolean check){
        if (check && !isGoodJson(jsonStr)){
            return null;
        }

        Gson gson = new Gson();
        return gson.fromJson(jsonStr,NewsReport.class);
    }

    public static boolean isGoodJson(String jsonStr){
        try {
            parser.parse(jsonStr);
            return true;
        }catch (Exception e) {
            return false;
        }
    }
}
