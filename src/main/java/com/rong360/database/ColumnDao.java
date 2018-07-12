package com.rong360.database;

import com.alibaba.druid.pool.DruidDataSource;
import com.rong360.binlogutil.GlobalConfig;
import net.sf.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author zhangtao@rong360.com
 */
public class ColumnDao {

    private static Logger log = LoggerFactory.getLogger(ColumnDao.class);
    private static final String SCHEMA_SQL = "SELECT column_name,ordinal_position,COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = ? AND table_name = ?";
    private static ConcurrentHashMap<String, HashMap<Integer, String>> tableColumnMap = new ConcurrentHashMap<>();

    private static final DruidDataSource dataSource = new DruidDataSource();

    public void removeCache(String dbName, String tableName) {

        String mapKey = dbName.toLowerCase() + ":" + tableName.toLowerCase();
        tableColumnMap.remove(mapKey);

    }

    public void clearCache() {
        tableColumnMap.clear();
    }

    static {
        try {
            dataSource.setUrl(String.format("jdbc:mysql://%s:%s?useUnicode=true&characterEncoding=UTF8", GlobalConfig.mysql_host, GlobalConfig.mysql_port));
            dataSource.setUsername(GlobalConfig.mysql_username);
            dataSource.setPassword(GlobalConfig.mysql_password);
            dataSource.setInitialSize(1);
            dataSource.setMaxActive(2);
            dataSource.setMinIdle(1);
            dataSource.setMinEvictableIdleTimeMillis(300 * 1000);
            dataSource.setTimeBetweenEvictionRunsMillis(180 * 1000);
            dataSource.setTestWhileIdle(true);
            dataSource.setTestOnBorrow(false);
            dataSource.setValidationQuery("SELECT 1");
        } catch (Exception e) {
            log.error("init data source", e);
        }
    }

    public static void getType(String type, Map<String, Object> map) {
        Pattern pattern = Pattern.compile("(enum)\\((.*)\\)", Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(type);
        map.put("type", type);
        String[] list = null;
        if (matcher.find()) {
            map.put("type", "enum");
            list = matcher.group(2).split(",");
        } else {
            pattern = Pattern.compile("(set)\\((.*)\\)", Pattern.CASE_INSENSITIVE);
            matcher = pattern.matcher(type);
            if (matcher.find()) {
                map.put("type", "set");
                list = matcher.group(2).split(",");
            }
        }
        if (list != null) {
            for (int i = 0; i < list.length; i++) {
                list[i] = list[i].substring(1, list[i].length() - 1);
            }
        }
        map.put("list", list);
    }

    public static void getSlaveStatus() {
        try (
                Connection conn = dataSource.getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SHOW SLAVE STATUS")) {

            String relayMasterLogFile = "", execMasterLogPos = "";
            while (rs.next()) {
                relayMasterLogFile = rs.getString("Relay_Master_Log_File");
                execMasterLogPos = rs.getString("Exec_Master_Log_Pos");
            }
            log.info("show slave status, master log file:{}, pos:{}", relayMasterLogFile, execMasterLogPos);
        } catch (Exception e) {
            log.error("db", e);
        }
    }

    public HashMap<Integer, String> getColumnByTable(String dbName, String tableName) {
        HashMap<Integer, String> result;

        String mapKey = dbName.toLowerCase() + ":" + tableName.toLowerCase();
        result = tableColumnMap.get(mapKey);
        if (result != null && result.size() > 0) {
            return result;
        }
        result = new HashMap<>();
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(SCHEMA_SQL)) {
            ps.setString(1, dbName);
            ps.setString(2, tableName);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs != null) {
                    while (rs.next()) {
                        Integer position = rs.getInt(2);
                        String columnName = rs.getString(1);
                        Map<String, Object> map = new HashMap<>();
                        map.put("name", columnName);
                        String type = rs.getString(3);
                        getType(type, map);
                        JSONObject obj = JSONObject.fromObject(map);
                        result.put(position - 1, obj.toString());
                    }
                }
                tableColumnMap.put(mapKey, result);
            }
        } catch (Exception e) {
            log.error("query db", e);
        }
        return result;
    }

}
