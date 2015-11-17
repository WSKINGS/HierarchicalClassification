package com.ws.util;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Administrator on 2015/11/17.
 */
public class StopWords implements Serializable {
    private static final long serialVersionUID = 8760072596561403270L;
    private static final String stopWordsFile = "stopwords.txt";

    private static Map<String,Boolean> words;

    static {
        words = new HashMap<String, Boolean>(1500);

        try {
            File file = new File(StopWords.class.getClassLoader().getResource(stopWordsFile).getFile());
            //System.out.println(file.getAbsoluteFile());
            BufferedReader reader = new BufferedReader(new FileReader(file));
            String line = null;
            while ( (line = reader.readLine()) != null ) {
                words.put(line,true);
            }
            reader.close();
        } catch (IOException e) {
            //do nothing
            e.printStackTrace();
        }
    }
    public static boolean isStopWord(String word){
        return words.containsKey(word);
    }
}
