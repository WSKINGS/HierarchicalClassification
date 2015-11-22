package com.ws.application;

import com.ws.classifier.NewsReportTransformation;
import com.ws.model.NewsReport;
import org.apache.spark.mllib.linalg.Vector;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by Administrator on 2015/11/22.
 */
public class ClassifierTest {

    @Test
    public void testClassifyNewsReport (){
        NewsReport news = new NewsReport();
        news.setTitle("市革命烈士陵园墓包将作保护改造");
        news.setContent("(生态）辽宁在全国率先启动“十一五”沿海防护\n" +
                "林建设工程\n" +
                "　　新华社北京4月15日电（记者姚润丰、董峻）国家林\n" +
                "业局15日宣布，全国沿海防护林体系建设重点地区之一的\n" +
                "辽宁省近日正式启动“十一五”沿海防护林体系建设工程，\n" +
                "在全国率先打响沿海防护林体系建设工程攻坚战。这标志\n" +
                "着我国“十一五”沿海防护林工程建设进入实施新阶段。\n" +
                "     国家林业局有关负责人介绍说，沿海防护林体系建\n" +
                "设工程继国家六大林业重点工程之后的又一项林业重点工\n" +
                "程，也是国家减灾防灾安全体系建设的重要内容。为全面\n" +
                "推进沿海防护林体系建设工程，国家林业局正在抓紧修编\n" +
                "《全国沿海防护林体系建设工程规划》《全国红树林保护\n" +
                "和发展规划》和《沿海湿地保护和恢复工程规划》，研究\n" +
                "制定沿海防护林条例及沿海防护林体系建设等相关技术标\n" +
                "准，即将出台《全国沿海防护林体系建设规程》。目前，\n" +
                "沿海防护林体系建设工程全面实施的各项准备工作已基本\n" +
                "就绪。\n" +
                "    据介绍，辽宁省位于我国万里海疆的最北端，海岸线\n" +
                "长度2292公里，占全国大陆海岸线总长的12.5%。这个省\n" +
                "沿海防护林体系建设工程，东起丹东鸭绿江口，西至绥中\n" +
                "县万家镇红石礁，涉及丹东、大连、鞍山、营口、盘锦、\n" +
                "锦州和葫芦岛7市28个县（市、区）。\n" +
                "    “十一五”期间，辽宁省计划投入47.7亿元，在工程\n" +
                "区完成人工造林220.5万亩，封山育林445.5万亩，低效林\n" +
                "改造202.5万亩，森林抚育174万亩，湿地恢复面积90.8万\n" +
                "亩，森林覆盖率提高9个百分点，到2010年达到51.6%，初\n" +
                "步建立起多功能、多层次的综合性防护林体系。（完）");

        //Vector v = NewsReportTransformation.changeNewsReport2Vector(spaceMap, news);
        //double score = model.predict(v);
        //System.out.println("score:"+score);
    }
}