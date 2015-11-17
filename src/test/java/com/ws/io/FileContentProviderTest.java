package com.ws.io;

import com.ws.model.NewsReport;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by Administrator on 2015/11/12.
 */
public class FileContentProviderTest {

    @Test
    public void testLoadNewsReports() throws Exception {
        Method method = FileContentProvider.class.getDeclaredMethod("loadNewsReports");
        method.setAccessible(true);
        List<NewsReport> news = (List<NewsReport>) method.invoke(new FileContentProvider());
        System.out.println(news.size());
    }

    @Test
    public void testFileExist() throws Exception {
        Field field = FileContentProvider.class.getDeclaredField("filename");
        field.setAccessible(true);
        String filename = (String) field.get(new FileContentProvider());
        File file = new File(filename);
        assertTrue(file.exists());
    }
}