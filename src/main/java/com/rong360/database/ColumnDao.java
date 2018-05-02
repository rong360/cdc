package com.rong360.database;

import com.rong360.binlogutil.GlobalConfig;
import net.sf.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
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
    private static ConcurrentHashMap<String, HashMap<Integer, String>> tableColumnMap = new ConcurrentHashMap<String, HashMap<Integer, String>>();

    public void removeCache(String dbName, String tableName) {

        String mapKey = dbName.toLowerCase() + ":" + tableName.toLowerCase();
        tableColumnMap.remove(mapKey);

    }

    public void clearCache() {
        tableColumnMap.clear();
    }


    public HashMap<Integer, String> getColumnByTable(String dbName, String tableName) {

        PreparedStatement ps = null;
        ResultSet rs = null;
        HashMap<Integer, String> result = null;

        String mapKey = dbName.toLowerCase() + ":" + tableName.toLowerCase();
        result = tableColumnMap.get(mapKey);
        if (result != null && result.size() > 0) {
            return result;
        }
        result = new HashMap<Integer, String>();
        String sql = "select column_name,ordinal_position,COLUMN_TYPE from INFORMATION_SCHEMA.COLUMNS where table_schema = ? and table_name = ?";
        Connection connection = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            String url = "jdbc:mysql://%s:%s/INFORMATION_SCHEMA?user=%s&password=%s&useUnicode=true&characterEncoding=UTF8";
            url = String.format(url, GlobalConfig.mysql_host, GlobalConfig.mysql_port, GlobalConfig.mysql_username, GlobalConfig.mysql_password);
            connection = DriverManager.getConnection(url);
            ps = connection.prepareStatement(sql);
            ps.setString(1, dbName);
            ps.setString(2, tableName);
            rs = ps.executeQuery();

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
            rs.close();
            ps.close();
            tableColumnMap.put(mapKey, result);

        } catch (ClassNotFoundException e) {
            log.error("db", e);
        } catch (SQLException e) {
            log.error("db", e);
        } finally {
            try {
                connection.close();
            } catch (SQLException e) {
                log.error("db close", e);
            }
        }
        return result;
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

    public static void main(String[] args) {
        String sql = "/*!40000 ALTER TABLE `apply_info` DISABLE KEYS */";
        sql = sql.replaceAll("`", "");
        Pattern pattern = Pattern.compile("(alter)(\\s+)(table)(\\s+)(\\w+)(\\s+)(.*)", Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(sql);
        String table = "";
        if (matcher.find()) {
            table = matcher.group(5);
            System.out.println(table);
        }
    }


}
