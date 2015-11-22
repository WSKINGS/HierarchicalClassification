package com.ws.io;

import com.ws.model.NewsReport;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Administrator on 2015/10/27.
 */
public class FileContentProvider implements ContentProvider,Serializable {
    private static final String filename = "D:\\temp\\train.trs3.xml";
    private static final long serialVersionUID = -8663230002104655584L;

    public JavaRDD<NewsReport> getSource(JavaSparkContext jsc) {
        List<NewsReport> newsReports = loadNewsReports();
        return jsc.parallelize(newsReports);
    }

    private List<NewsReport> loadNewsReports() {
        try {
            Document doc = Jsoup.parse(new File(filename), "utf8");
            Elements elements = doc.select("doc");
            List<NewsReport> list = new ArrayList<NewsReport>(elements.size());
            for (Element elem : elements) {
                NewsReport report = new NewsReport();
                report.setId(elem.attr("id"));
                report.setTitle(elem.select("title").text());
                report.setContent(elem.select("content").text());
                report.setCatId(elem.select("ccnc_cat").text());
                report.setCatLable(elem.select("ccnc_label").text());
                list.add(report);
            }
            return list;
        } catch (IOException e) {
            e.printStackTrace();
            return new ArrayList<NewsReport>();
        }
    }
}
