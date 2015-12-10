package com.ws.assistant;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Administrator on 2015/12/10.
 */
public class TestDataFormat {

    private static final String srcPath = "D:\\temp\\testdata6\\test-06-04.trs";
    private static final String labelPath = "D:\\temp\\testdata6\\CorrectAnswer.txt";
    private static final String destPath = "D:\\temp\\testdata6\\labeledTestData.xml";
    private static final String jsonPath = "D:\\temp\\testdata6\\labeledTestData.json";

    public static void main ( String[] args ) {
        TestDataFormat format = new TestDataFormat();
        format.labelTestData(srcPath,labelPath,destPath);
    }

    private void labelTestData(String srcfile, String labelFIle,String destFile){

        try {
            Map<String,String> map = new HashMap<String, String>(11577);
            BufferedReader reader = new BufferedReader(new FileReader(labelFIle));
            String line = null;
            while ((line=reader.readLine()) !=null) {
                if ("".equals(line.trim())){
                    continue;
                }
                String[] arr = line.split("\t");
                if (arr.length != 3) {
                    System.out.println("format error! " + line);
                    continue;
                }

                map.put(arr[0], arr[1]+"."+arr[2]);
            }

            reader.close();

            Document doc = Jsoup.parse(new File(srcfile), "utf8");
            Elements elements = doc.select("doc");
            BufferedWriter jsonWriter = new BufferedWriter(new FileWriter(jsonPath));
            BufferedWriter xmlWriter = new BufferedWriter(new FileWriter(destFile));
            for (Element elem : elements) {
                String id = elem.attr("id");
                elem.append("<ccnc_cat id=\""+id+"\">"+map.get(id)+"</ccnc_cat>\n" +
                        "<ccnc_label id=\""+id+"\">"+map.get(id)+"</ccnc_label>");
                xmlWriter.write(elem.toString());

                StringBuffer sb = new StringBuffer();
                sb.append("{").append("\"id\":").append("\"").append(id).append("\"").append(",");
                sb.append("\"title\":").append("\"").append(elem.select("title").text().replace("\"", "'")).append("\"").append(",");
                sb.append("\"content\":").append("\"").append(elem.select("content").text().replace("\"", "'")).append("\"").append(",");
                sb.append("\"ccnc_cat\":").append("\"").append(map.get(id)).append("\"").append(",");
                sb.append("\"ccnc_label\":").append("\"").append(map.get(id)).append("\"").append("}");
                jsonWriter.write(sb.toString()+"\n");
            }

            xmlWriter.flush();
            xmlWriter.close();
            jsonWriter.flush();
            jsonWriter.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
