package com.rong360.binlogutil;

import com.rong360.model.DeleteQueueData;
import com.rong360.model.InsertQueueData;
import com.rong360.model.UpdateQueueData;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author zhangtao@rong360.com
 */

public class EventDataUtil {

    public static ConcurrentHashMap<Long, String> tableIdMap = new ConcurrentHashMap<>();
    public static Logger log = LoggerFactory.getLogger("queue");


    public static void pushToMap(long tableId, String dbName, String tableName) {
        String orgName = tableIdMap.get(tableId);
        if (orgName != null) {
            if (orgName.equals(dbName + "|" + tableName)) {
                return;
            } else {
                log.warn("table id conflict occur!");
                tableIdMap.put(tableId, dbName + "|" + tableName);
            }
        } else {
            tableIdMap.put(tableId, dbName + "|" + tableName);
        }
    }

    public static String[] getDbTableName(long tableId) {
        String orgName = tableIdMap.get(tableId);
        if (orgName != null && !orgName.equals(""))
            return orgName.split("\\|");
        return null;

    }

    public static ArrayList<String> deleteQueueToJson(long tableId, ArrayList<DeleteQueueData> deleteList, String binloginfo) {
        String[] oriName = getDbTableName(tableId);
        String dbName = oriName[0];
        String tableName = oriName[1];

        Map<String, Object> map = new HashMap<String, Object>();
        ArrayList<String> result = new ArrayList<String>();
        for (DeleteQueueData dqd : deleteList) {
            map.put("database", dbName);
            map.put("table", tableName);
            map.put("action", "delete");
            map.put("data", dqd.getAftertDelete());
            map.put("binloginfo", binloginfo);
            map.put("uniqid", RongUtil.getMd5(map.toString()));
            map.put("createtime", System.currentTimeMillis());
            JSONObject obj = JSONObject.fromObject(map);
            result.add(obj.toString());
            log.info(obj.toString());
        }
        return result;
    }

    public static ArrayList<String> insertQueueTojson(long tableId, ArrayList<InsertQueueData> insertList, String binloginfo) {
        String[] oriName = getDbTableName(tableId);
        String dbName = oriName[0];
        String tableName = oriName[1];
        Map<String, Object> map = new HashMap<String, Object>();
        ArrayList<String> result = new ArrayList<String>();
        for (InsertQueueData iqd : insertList) {
            map.put("database", dbName);
            map.put("table", tableName);
            map.put("action", "insert");
            map.put("data", iqd.getAfterInsert());
            map.put("binloginfo", binloginfo);
            map.put("uniqid", RongUtil.getMd5(map.toString()));
            map.put("createtime", System.currentTimeMillis());
            JSONObject obj = JSONObject.fromObject(map);
            result.add(obj.toString());
            log.info(obj.toString());
        }
        return result;
    }

    public static ArrayList<String> updateQueueToJson(long tableId, ArrayList<UpdateQueueData> updateList, String binloginfo) {
        String[] oriName = getDbTableName(tableId);
        String dbName = oriName[0];
        String tableName = oriName[1];
        Map<String, Object> map = new HashMap<String, Object>();
        Map<String, Object> dataMap = new HashMap<String, Object>();
        ArrayList<String> result = new ArrayList<String>();
        for (UpdateQueueData uqd : updateList) {
            map.put("database", dbName);
            map.put("table", tableName);
            map.put("action", "update");
            dataMap.put("before", uqd.getBeforeUpdate());
            dataMap.put("after", uqd.getAfterUpdate());
            map.put("data", dataMap);
            map.put("binloginfo", binloginfo);
            map.put("uniqid", RongUtil.getMd5(map.toString()));
            map.put("createtime", System.currentTimeMillis());
            JSONObject obj = JSONObject.fromObject(map);
            result.add(obj.toString());
            log.info(obj.toString());
        }
        return result;
    }

    public static ArrayList<InsertQueueData> getInsertEventData(BitSet includedColumns, List<Serializable[]> rows, HashMap<Integer, String> columnMap) {
        ArrayList<InsertQueueData> resultList = new ArrayList<InsertQueueData>();
        for (Serializable[] row : rows) {
            InsertQueueData iqd = new InsertQueueData();
            iqd.setAfterInsert(getMyMap(row, includedColumns, columnMap));
            resultList.add(iqd);
        }
        return resultList;

    }

    private static HashMap<String, Object> getMyMap(Serializable[] row, BitSet includedColumns, HashMap<Integer, String> columnMap) {
        HashMap<String, Object> mymap = new HashMap<>();
        String value;
        int j = 0;
        for (int i = includedColumns.nextSetBit(0); i >= 0; i = includedColumns.nextSetBit(i + 1)) {
            JSONObject info = JSONObject.fromObject(columnMap.get(i));
            value = convert(row, j++);
            value = convertByType(value, info.getString("type"), info.get("list"));
            mymap.put(info.getString("name"), value);
        }
        return mymap;
    }

    private static String convertByType(String value, String type, Object list) {
        if (!(list instanceof JSONArray)) {
            return value;
        }
        try {
            int intValue = 0;
            switch (type) {
                case "enum":
                    intValue = Integer.parseInt(value);
                    if (intValue == 0) {
                        value = "";
                        break;
                    }
                    value = ((JSONArray) list).getString(intValue - 1);
                    break;
                case "set":
                    intValue = Integer.parseInt(value);
                    if (intValue == 0) {
                        value = "";
                        break;
                    }
                    StringBuilder valueT = new StringBuilder();
                    int i = 0, j;
                    while (intValue > 0) {
                        j = intValue & ((int) Math.pow(2, i));
                        if (j > 0) {
                            valueT.append(((JSONArray) list).getString(i));
                            intValue = intValue - j;
                            if (intValue > 0) {
                                valueT.append(",");
                            }
                        }
                        i++;
                    }
                    if (!valueT.toString().isEmpty()) {
                        value = valueT.toString();
                    }
                    break;
            }
        } catch (Exception e) {
            log.warn("convertByType error", e);
        } finally {
            return value;
        }
    }

    public static ArrayList<DeleteQueueData> getDeleteEventData(BitSet includedColumns, HashMap<Integer, String> columnMap, List<Serializable[]> rows) {
        ArrayList<DeleteQueueData> resultList = new ArrayList<>();
        for (Serializable[] row : rows) {
            DeleteQueueData dqd = new DeleteQueueData();
            dqd.setAftertDelete(getMyMap(row, includedColumns, columnMap));
            resultList.add(dqd);
        }
        return resultList;

    }

    public static ArrayList<UpdateQueueData> getUpdateEventData(BitSet beforeColumns, BitSet includedColumns, HashMap<Integer, String> columnMap, List<Map.Entry<Serializable[], Serializable[]>> rows) {
        ArrayList<UpdateQueueData> resultList = new ArrayList<UpdateQueueData>();
        for (Map.Entry<Serializable[], Serializable[]> row : rows) {
            UpdateQueueData uqd = new UpdateQueueData();
            uqd.setBeforeUpdate(getMyMap(row.getKey(), beforeColumns, columnMap));
            uqd.setAfterUpdate(getMyMap(row.getValue(), includedColumns, columnMap));
            resultList.add(uqd);
        }
        return resultList;
    }

    public static String convert(Serializable[] tmp, int j) {
        String translateValue = "";
        try {
            Serializable c = tmp[j];
            if (c == null) {
                translateValue = null;
            } else if (c instanceof byte[]) {
                translateValue = new String((byte[]) c, "UTF-8");
            } else if (c instanceof java.sql.Timestamp) {
                translateValue = String.valueOf(((java.sql.Timestamp) c).getTime());
            } else if (c instanceof java.sql.Date) {
                translateValue = String.valueOf(((java.sql.Date) c).getTime());
            } else if (c instanceof BitSet) {
                int result = 0;
                for (int i = ((BitSet) c).nextSetBit(0); i >= 0; i = ((BitSet) c).nextSetBit(i + 1)) {
                    if (i > 0) {
                        result += 1 << i;
                    } else {
                        result += 1;
                    }
                }
                translateValue = String.valueOf(result);
            } else {
                translateValue = String.valueOf(c);
            }
        } catch (UnsupportedEncodingException e) {
            log.warn("convert", e);
        }

        return translateValue;
    }
}
