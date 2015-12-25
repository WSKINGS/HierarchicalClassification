package com.ws.singleMachine;

import com.ws.io.FileContentProvider;
import com.ws.model.Feature;
import com.ws.model.NewsReport;
import com.ws.util.Parameters;
import com.ws.util.Segment;
import com.ws.util.StopWords;
import org.ansj.domain.Term;
import org.apache.log4j.Logger;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

/**
 * Created by Administrator on 2015/12/23.
 */
public class FeatureSelector {

    private static Logger logger = Logger.getLogger(FeatureSelector.class);

    private List<Feature> getFeatures(List<NewsReport> list){

        int total = list.size();

        Map<String,Integer> hierarchicalMap = new HashMap<String, Integer>();
        Map<String, List<WordDocFeature>> map = new HashMap<String, List<WordDocFeature>>();
        for (NewsReport newsReport : list) {

            String father = newsReport.getCcnc_cat().split("[.]")[0];
            if (!hierarchicalMap.containsKey(father)) {
                hierarchicalMap.put(father,1);
            } else {
                hierarchicalMap.put(father,hierarchicalMap.get(father)+1);
            }
            if (!hierarchicalMap.containsKey(newsReport.getCcnc_cat())) {
                hierarchicalMap.put(newsReport.getCcnc_cat(),1);
            } else {
                hierarchicalMap.put(newsReport.getCcnc_cat(), hierarchicalMap.get(newsReport.getCcnc_cat())+1);
            }

            List<Term> terms = Segment.segNewsreport(newsReport);
            Map<String,Integer> tfMap = new HashMap<String, Integer>();
            for (Term term : terms) {
                if (StopWords.isStopWord(term.getName())){
//                    logger.info("stop word: "+term.getName());
                    System.out.println("stop word: " + term.getName());
                    continue;
                }

                if (tfMap.containsKey(term.getName())) {
                    tfMap.put(term.getName(), tfMap.get(term.getName())+1);
                } else {
                    tfMap.put(term.getName(),1);
                }
            }

            for (String word : tfMap.keySet()) {
                WordDocFeature docFeature = new WordDocFeature();
                docFeature.setDocId(newsReport.getId());
                docFeature.setCategory(newsReport.getCcnc_cat());
                docFeature.setWord(word);
                docFeature.setTf(tfMap.get(word));

                if (!map.containsKey(word)){
                    map.put(word, new ArrayList<WordDocFeature>());
                }
                map.get(word).add(docFeature);
            }
        }

        List<Feature> features = new ArrayList<Feature>();
        for (String category : hierarchicalMap.keySet()) {
            for (String word : map.keySet()) {
                List<WordDocFeature> temp = map.get(word);
                int trueClass = 0;
                for (WordDocFeature docFeature : temp) {
                    if (docFeature.getCategory().startsWith(category)){
                        trueClass++;
                    }
                }

                int df = temp.size();
                if (df < Parameters.dfThreshold) {
//                    logger.info("df filter: "+word);
                    System.out.println("df filter: " + word);
                    continue;
                }

                double mi = Math.log((trueClass*total+1.0)*1.0/(df*hierarchicalMap.get(category)+1.0));
                Feature feature = new Feature();
                feature.setWord(word);
                feature.setMi(mi);
                feature.setIdf(Math.log(total/df));
                feature.setTf(df);
                features.add(feature);
            }
        }

        return features;
    }

    public static void main ( String[] args ) throws IOException {
        FileContentProvider provider = new FileContentProvider();
        List<NewsReport> list = provider.loadNewsReports(Parameters.filepath);
        FeatureSelector selector = new FeatureSelector();
        List<Feature> features =  selector.getFeatures(list);

        list.clear();
        list = null;

        features = selector.topK(features,Parameters.TopN);

        BufferedWriter writer = new BufferedWriter(new FileWriter("features"));
        for (Feature feature : features) {
            writer.write(feature.toString()+"\n");
        }
        writer.close();

    }

    private List<Feature> topK(List<Feature> features, double miThreshold) {
        if (features==null || miThreshold >= features.size()) {
            return features;
        }

        int index = new Random(System.currentTimeMillis()).nextInt(features.size());
        double piror = features.get(index).getMi();
        List<Feature> less = new ArrayList<Feature>();
        List<Feature> equal = new ArrayList<Feature>();
        List<Feature> more = new ArrayList<Feature>();
        for (Feature feature : features) {
            if (feature.getMi() < piror ) {
                less.add(feature);
            } else if (feature.getMi() > piror){
                more.add(feature);
            } else {
                equal.add(feature);
            }
        }

        if (more.size() >= miThreshold) {
            return topK(more,miThreshold);
        } else if (more.size() + equal.size() >= miThreshold) {
            for (int i=0; i < miThreshold - more.size(); i++) {
                more.add(equal.get(i));
            }
            return more;
        }

        more.addAll(equal);
        List<Feature> temp = topK(less, miThreshold-more.size());
        more.addAll(temp);
        return more;
    }
}
