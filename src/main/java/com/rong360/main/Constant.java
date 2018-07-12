package com.rong360.main;

/**
 * Constant definition
 *
 * @author liuchi
 * 2018/4/9
 */
public class Constant {
    //Register etcd successfully
    public static final String REGISTER_STATUS_OK = "1";
    //Registration successful and get lock running
    public static final String REGISTER_STATUS_RUN = "2";
    //filter online table identification
    public static final String FILTER_TABLE_ONLINE = "1";
    //thread type performance
    public static final int THREAD_TYPE_PER = 1;
    //thread type sequence
    public static final int THREAD_TYPE_SEQ = 2;
    //thread type others
    public static final int THREAD_TYPE_OTH = 3;
}
