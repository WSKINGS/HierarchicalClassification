package com.ws.assistant;

import scala.Int;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Administrator on 2015/12/5.
 */
public class TestDadaAnalysizer {
    private static final String testFilePath = "D:\\分类论文\\NLPCC\\testAnnotation\\CorrectAnswer.txt";
    private static final String outFile = "testDataChild.csv";

    public static void main ( String[] args ) throws IOException {
        TestDadaAnalysizer analysizer = new TestDadaAnalysizer();
        Map map = analysizer.statistic(testFilePath);
        outPut(map, outFile);
    }

    private static void outPut(Map<String, Map<String , Integer>> map, String outFile) throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter(outFile));

        for (String key : map.keySet()) {
            int total = 0;
            Map<String,Integer> secondMap = map.get(key);
            for (String second : secondMap.keySet()){
                writer.write(key+"_"+second+","+secondMap.get(second)+"\n");
                //total += secondMap.get(second);
            }
            //writer.write(key+",total,"+total+"\n");
        }

        writer.flush();
        writer.close();
    }

    public Map<String,Map<String,Integer>> statistic(String filename){
        Map<String,Map<String,Integer>> map = new HashMap<String, Map<String, Integer>>(11577);

        try {
            BufferedReader reader = new BufferedReader(new FileReader(filename));
            String line = null;
            while ((line=reader.readLine())!=null){
                if ("".equals(line.trim())){
                    continue;
                }
                String[] arr = line.split("\t");
                if (!map.containsKey(arr[1])){
                    Map<String,Integer> temp = new HashMap<String, Integer>();
                    temp.put(arr[2],1);
                    map.put(arr[1], temp);
                }
                else {
                    Map<String, Integer> temp = map.get(arr[1]);
                    if (!temp.containsKey(arr[2])){
                        temp.put(arr[2],1);
                    } else {
                        temp.put(arr[2], temp.get(arr[2])+1);
                    }
                }
            }

            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return map;
    }
}
