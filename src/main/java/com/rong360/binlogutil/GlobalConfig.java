package com.rong360.binlogutil;

import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.data.KeyValue;
import com.coreos.jetcd.options.GetOption;
import com.mysql.jdbc.StringUtils;
import com.rong360.etcd.EtcdClient;
import com.rong360.main.Constant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author zhangtao@rong360.com
 */
public class GlobalConfig {

    public static String mysql_host = null;
    public static Integer mysql_port = null;
    public static String mysql_username = null;
    public static String mysql_password = null;

    public static String rabbitmq_host = null;
    public static Integer rabbitmq_port = null;
    public static String rabbitmq_vhost = null;
    public static String rabbitmq_username = null;
    public static String rabbitmq_password = null;
    public static String rabbitmq_exchangename = null;

    public static Integer ping_failed_maxretry = null;
    public static Integer connect_timedout = null;
    public static String  mysqlTimeZone = "GMT+8";

    public static boolean isNeedperThread = false;
    public static boolean isNeedseqThread = false;

    public static String[] etcd_host = null;
    public static String etcd_username = null;
    public static String etcd_password = null;

    public static volatile ConcurrentHashMap<String, Integer> filterMap = new ConcurrentHashMap<String, Integer>();
    public static volatile ConcurrentHashMap<String, Integer> suffixMap = new ConcurrentHashMap<String, Integer>();

    private final static Logger logger = LoggerFactory.getLogger(GlobalConfig.class);
    private final static ReadWriteLock lock = new ReentrantReadWriteLock();//lock
    private final static Integer FILTER_TYPE_PER =1;
    private final static Integer FILTER_TYPE_SEQ =2;

    public static boolean filterByTableId(long tableId) {

        String hash[] = EventDataUtil.getDbTableName(tableId);
        if (hash == null) {
            return false;
        }
        String dbName = hash[0];
        String tblName = hash[1];
        Integer ret = filterTable(dbName, tblName);
        return ret > 0 ? true : false;

    }

    /**
     * @param dbName:database name
     * @param tblName：table name
     * @return 1：Hit performance prioritization configuration 2：Hit seq priority configuration configuration 0：Missed
     */
    public static Integer filterTable(String dbName, String tblName) {
        lock.readLock().lock();
        String fullName = RongUtil.generateTableKey(dbName, tblName);
        try {
            Integer result = filterMap.get(fullName);
            if (result != null) {
                return result;
            }
            for (String s : suffixMap.keySet()) {
                if (fullName.indexOf(s) == 0) {
                    if (suffixMap.get(s) == FILTER_TYPE_SEQ) {
                        filterMap.put(fullName, FILTER_TYPE_SEQ);
                        logger.info(fullName + " match:" + s + "{suffix},stored into seq priority configuration");
                        return FILTER_TYPE_SEQ;
                    } else if (suffixMap.get(s) == FILTER_TYPE_PER) {
                        filterMap.put(fullName, FILTER_TYPE_PER);
                        logger.info(fullName + " match:" + s + "{suffix},stored into performance priority configuration");
                        return FILTER_TYPE_PER;
                    }
                }
            }
            return 0;
        } finally {
            lock.readLock().unlock();
        }

    }

    private static void logTableInfo(boolean prefix) {
        ConcurrentHashMap<String, Integer> tmpFilterMap;
        if (prefix){
            tmpFilterMap = suffixMap;
        }else{
            tmpFilterMap = filterMap;
        }
        StringBuffer sb = new StringBuffer();
        sb.append("==========start===========\n");
        sb.append("********monitor "+(prefix?"prefix match":"")+" table********\n");
        boolean print = false;
        StringBuffer xn = new StringBuffer();
        StringBuffer sx = new StringBuffer();
        for (String s : tmpFilterMap.keySet()) {
            print = true;
            if (tmpFilterMap.get(s) == FILTER_TYPE_PER) {
                xn.append(s + (prefix?"{suffix}":"")+"\n");
            } else if (tmpFilterMap.get(s) == FILTER_TYPE_SEQ) {
                sx.append(s + (prefix?"{suffix}":"")+"\n");
            }
        }
        if (xn.length()>0){
            sb.append("performance first:\n" + xn);
        }
        if (sx.length()>0){
            sb.append("seq first:\n" + sx);
        }
        sb.append("==========end===========");
        if (print) {
            logger.info(sb.toString());
        }
    }

    public synchronized static void loadCdcConf() throws Exception {
        lock.writeLock().lock();
        try {
            String cdcConfig = RongUtil.etcdPrefix() + "config/cdc";
            List<KeyValue> list = EtcdClient.getInstance().getKVClient().
                    get(
                            ByteSequence.fromString(cdcConfig),
                            GetOption.newBuilder().withPrefix(ByteSequence.fromString(cdcConfig)).build()).
                    get().
                    getKvs();
            filterMap.clear();
            suffixMap.clear();
            for (KeyValue keyValue : list) {
                String[] tmpKey = keyValue.getKey().toStringUtf8().split("/");
                String tmpVal = keyValue.getValue().toStringUtf8();
                if (StringUtils.isEmptyOrWhitespaceOnly(tmpVal)){
                    continue;
                }
                switch (tmpKey[5]){
                    case "ping_failed_max_retry" :
                        ping_failed_maxretry = Integer.valueOf(tmpVal);
                        break;
                    case "connection_timedout_inseconds" :
                        connect_timedout = Integer.valueOf(tmpVal);
                        break;
                    case "mysqlTimeZone" :
                        mysqlTimeZone = tmpVal;
                        break;
                    case "filter" :
                        if (tmpVal.equals(Constant.FILTER_TABLE_ONLINE)){
                            isNeedperThread = true;
                            generateMap(tmpKey[6], tmpKey[7], 1);
                        }
                        break;
                    case "seqfilter" :
                        if (tmpVal.equals(Constant.FILTER_TABLE_ONLINE)){
                            isNeedseqThread = true;
                            generateMap(tmpKey[6], tmpKey[7], 2);
                        }
                        break;
                }
            }
            if (filterMap.size() == 0  && suffixMap.size() == 0) {
                throw new Exception("filter or seqfilter config is empty!");
            }
            logTableInfo(false);
            logTableInfo(true);
        } finally {
            lock.writeLock().unlock();
        }

    }

    public static void loadConfig() throws Exception {
        String appConfig = RongUtil.etcdPrefix() + "config/app";
        List<KeyValue> list = EtcdClient.getInstance().getKVClient().get(ByteSequence.fromString(appConfig),
                GetOption.newBuilder().withPrefix(ByteSequence.fromString(appConfig)).build()).get().getKvs();
        Class<?> c = Class.forName("com.rong360.binlogutil.GlobalConfig");
        Constructor<?> con = c.getConstructor();
        Object obj = con.newInstance();
        for (KeyValue keyValue : list) {
            String[] tmpKey = keyValue.getKey().toStringUtf8().split("/");
            String tmpVal = keyValue.getValue().toStringUtf8();
            Field f = c.getField(tmpKey[tmpKey.length - 2] + "_" + tmpKey[tmpKey.length - 1]);
            if (tmpKey[tmpKey.length - 1].equals("port")) {
                f.set(obj, Integer.parseInt(tmpVal));
            } else {
                f.set(obj, tmpVal);
            }
        }
        loadCdcConf();
    }

    public static void generateMap(String database, String table, int type) throws Exception{
        int x = table.indexOf("{suffix}");
        ConcurrentHashMap<String, Integer> tmpFilterMap;
        String key = RongUtil.generateTableKey(database, table);
        if (x > 0) {
            tmpFilterMap = suffixMap;
            key = RongUtil.generateTableKey(database, table.substring(0, x));
        }else{
            tmpFilterMap = filterMap;
        }
        if (suffixMap.get(key)!=null ||
                filterMap.get(key)!=null) {
            throw new Exception(database + "." + table + " exists both in seqfilter and filter");
        }else{
            tmpFilterMap.put(key, type);
        }
    }
}
