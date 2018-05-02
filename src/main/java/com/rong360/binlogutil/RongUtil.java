package com.rong360.binlogutil;

import com.rong360.main.CdcClient;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RongUtil {
    private final static Logger logger = LoggerFactory.getLogger(RongUtil.class);

    public static String getBinlogFileKey() {
        return etcdPrefix() + "binlogfile/name";
    }

    public static String getBinlogPosKey() {
        return etcdPrefix() + "binlogfile/pos";
    }

    //If a subtable is configured, the tablName in the routing key is {table prefix}
    public static String getRoutingKey(String dbName, String tblName, String action) {
        String fullName = generateTableKey(dbName, tblName);
        for (String s : GlobalConfig.suffixMap.keySet()) {
            if (fullName.startsWith(s) && fullName.length() > s.length()) {
                tblName = "{" + fullName.substring(dbName.length() + 1, s.length()) + "}";
                return dbName + "." + tblName + "." + action;
            }
        }
        return dbName + "." + tblName + "." + action;
    }

    public static String etcdPrefix() {
        return CdcClient.uniEtcdKeyPrefix + "/" + CdcClient.cluster + "/" + CdcClient.instance + "/";
    }

    public static String generateTableKey(String database, String table) {
        return database + "." + table;
    }

    public static String getLockKey() {
        return etcdPrefix() + "lock";
    }

    public static String getRegisterKey() {
        return etcdPrefix() + "register/" + CdcClient.ip + "/" + CdcClient.pid;
    }

    /**
     * Generate md5
     *
     * @param source md5 string
     * @return md5
     * @author liuchi
     * 20180201
     */
    public static String getMd5(String source) {
        try {
            return DigestUtils.md5Hex(source.getBytes("UTF-8"));
        } catch (Exception e) {
            logger.error("getMD5 error", e);
        }
        return "";
    }
}
