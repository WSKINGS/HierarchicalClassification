package com.ws.classifier;

import com.ws.model.NewsReport;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.linalg.Vector;

import java.util.Map;

import static org.junit.Assert.*;

/**
 * Created by Administrator on 2015/12/10.
 */
public class SvmClassifierTest {

    private static void testClassify(SVMModel model, Map<String, Integer> spaceMap, Map<String, Double> idfMap) {
        NewsReport news = new NewsReport();
        news.setTitle("市革命烈士陵园墓包将作保护改造");
        news.setContent("本报讯记者从市民政部门获悉，为深切缅怀汕头市在各个革命历史时期为国捐躯的烈士们，更好地展现烈士墓风采，" +
                "汕头市将对汕头市革命烈士陵园墓包进行保护改造。 \n" +
                "汕头市革命烈士陵园位于\uE40B石风景区，始建于1959年4月。1961年6月，烈士墓包、休息室、拱桥、溢洪坝和道池落成。" +
                "该烈士墓包是全省唯一一座墓包形状的纪念建筑物，特色明显。由于建设年代久远，长期经受风雨侵蚀，墓包主体渗漏，为适用祭扫革命烈士墓活动的需要，" +
                "亟需对烈士墓包进行改造和设计。 \n" +
                "据悉，此次改造将采取“修旧如旧”的方式，既要突出历史的纪念意义及陵园文化底蕴相融合，又要具有时代特色；总体风格将保持庄严、" +
                "古朴、新颖；烈士墓包的材质、颜色、造型等要与陵园景观环境相协调。改造工程将广泛听取社会各界意见，近日市民政局已向全市征求汕头市革命烈士陵园墓包保护改造设计方案" +
                "及建议，包括平面布局、立面设计和整体实施效果等，有意者可将设计方案和建议送市民政局优抚安置科。（李凯）");

        news = new NewsReport();
        news.setTitle("尼勒克短信传递造林进度");
        news.setContent("本报讯　“截至4月6日，尼勒克县共完成人工造林3.58万亩。造林\n" +
                "进度较快的乡有……”4月7日下午，新疆维吾尔自治区伊犁州林业局局\n" +
                "领导，以及各处室负责人的手机收到全县造林进度的短信。今年，伊犁\n" +
                "州给尼勒克县下达的人工造林任务为2.4万亩，而县里自定的任务是4万\n" +
                "亩。全县70多个县直机关、企事业单位2300人，奔赴全县12个乡（镇、\n" +
                "场），完成1.4万亩的造林任务，占全县总任务量的三成以上。截至目\n" +
                "前，全县已完成造林3.58万亩。（刘河新;曹爱新）");

    }
}