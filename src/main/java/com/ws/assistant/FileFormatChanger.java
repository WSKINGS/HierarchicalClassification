package com.ws.assistant;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.ws.model.NewsReport;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Administrator on 2015/11/26.
 */
public class FileFormatChanger {
    public static void main ( String[] args ) {
        String filename = "D:\\temp\\train.trs.xml";
        String destFile = "D:/temp/train.json";

        FileFormatChanger changer = new FileFormatChanger();
        changer.testGson();
//        changer.changeXmlToJson(filename,destFile);
    }

    public void changeXmlToJson(String src, String dest){
        try {
            Document doc = Jsoup.parse(new File(src), "utf8");
            Elements elements = doc.select("doc");
            BufferedWriter writer = new BufferedWriter(new FileWriter(dest));
            for (Element elem : elements) {
                StringBuffer sb = new StringBuffer();
                sb.append("{").append("\"id\":").append("\"").append(elem.attr("id")).append("\"").append(",");
                sb.append("\"title\":").append("\"").append(elem.select("title").text().replace("\"", "'")).append("\"").append(",");
                sb.append("\"content\":").append("\"").append(elem.select("content").text().replace("\"", "'")).append("\"").append(",");
                sb.append("\"ccnc_cat\":").append("\"").append(elem.select("ccnc_cat").text().replace("．",".")).append("\"").append(",");
                sb.append("\"ccnc_label\":").append("\"").append(elem.select("ccnc_label").text().replace("\"","'")).append("\"").append("}");
                writer.write(sb.toString()+"\n");
            }

            writer.flush();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void testGson(){
        String jsonStr = "{\"id\":\"37980\",\"title\":\"(生态）辽宁在全国率先启动“十一五”沿海防护林建设工程\",\"content\":\"(生态）辽宁在全国率先启动“十一五”沿海防护 林建设工程 　　新华社北京4月15日电（记者姚润丰、董峻）国家林 业局15日宣布，全国沿海防护林体系建设重点地区之一的 辽宁省近日正式启动“十一五”沿海防护林体系建设工程， 在全国率先打响沿海防护林体系建设工程攻坚战。这标志 着我国“十一五”沿海防护林工程建设进入实施新阶段。 国家林业局有关负责人介绍说，沿海防护林体系建 设工程继国家六大林业重点工程之后的又一项林业重点工 程，也是国家减灾防灾安全体系建设的重要内容。为全面 推进沿海防护林体系建设工程，国家林业局正在抓紧修编 《全国沿海防护林体系建设工程规划》《全国红树林保护 和发展规划》和《沿海湿地保护和恢复工程规划》，研究 制定沿海防护林条例及沿海防护林体系建设等相关技术标 准，即将出台《全国沿海防护林体系建设规程》。目前， 沿海防护林体系建设工程全面实施的各项准备工作已基本 就绪。 据介绍，辽宁省位于我国万里海疆的最北端，海岸线 长度2292公里，占全国大陆海岸线总长的12.5%。这个省 沿海防护林体系建设工程，东起丹东鸭绿江口，西至绥中 县万家镇红石礁，涉及丹东、大连、鞍山、营口、盘锦、 锦州和葫芦岛7市28个县（市、区）。 “十一五”期间，辽宁省计划投入47.7亿元，在工程 区完成人工造林220.5万亩，封山育林445.5万亩，低效林 改造202.5万亩，森林抚育174万亩，湿地恢复面积90.8万 亩，森林覆盖率提高9个百分点，到2010年达到51.6%，初 步建立起多功能、多层次的综合性防护林体系。（完）\",\"ccnc_cat\":\"14.18\",\"ccnc_label\":\"农业、农村|林业\"}";
        JsonParser parser = new JsonParser();

        JsonObject object = (JsonObject) parser.parse(jsonStr);
        String id = object.get("id").getAsString();
        System.out.println("id:"+id);
        System.out.println("title:"+object.get("title"));
        System.out.println("content:"+object.get("content"));
        System.out.println("ccnc_cat:"+object.get("ccnc_cat"));
        System.out.println("ccnc_label:"+object.get("ccnc_label"));


    }
}
