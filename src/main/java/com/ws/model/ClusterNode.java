package com.ws.model;

import java.io.Serializable;
import java.util.List;

/**
 * Created by Administrator on 2015/10/27.
 */
public class ClusterNode implements Serializable {
    private static final long serialVersionUID = -3916968515664748094L;
    private String center;
    List<NewsReport> list;
}
