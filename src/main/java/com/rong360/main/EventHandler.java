package com.rong360.main;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.*;
import com.rong360.binlogutil.BinlogInfo;
import com.rong360.binlogutil.EventDataUtil;
import com.rong360.binlogutil.RongUtil;
import com.rong360.database.ColumnDao;
import com.rong360.etcd.EtcdApi;
import com.rong360.model.QueueData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author zhangtao@rong360.com
 * Event event processing thread
 */
public class EventHandler extends Thread {

    private final static Logger log = LoggerFactory.getLogger(EventHandler.class);
    private static LinkedBlockingQueue<Event> EventQueuePer = new LinkedBlockingQueue<>();
    private static LinkedBlockingQueue<Event> EventQueueSeq = new LinkedBlockingQueue<>();
    private static ConcurrentHashMap<String, BinlogInfo> threadBinlogMap =
            new ConcurrentHashMap<>();
    private static ConcurrentHashMap<String, String> threadWaitBinlogMap = new ConcurrentHashMap<>();
    private BinaryLogClient client;
    private String threadName;
    private List<QueueData> dataList = new ArrayList<>();
    private Integer threadType;

    public EventHandler(String name, BinaryLogClient client, Integer threadType) {
        this.threadName = name;
        this.client = client;
        this.threadType = threadType;
    }

    public static void pushPerQueue(Event event) {
        boolean bl = EventQueuePer.offer(event);
        if (EventQueuePer.size() > 100)
            log.info("EventQueuePer size=" + EventQueuePer.size());
        if (!bl) {
            log.error("EventQueuePer is full,size=" + EventQueuePer.size());
        }
    }

    public static void pushSeqQueue(Event event) {
        boolean bl = EventQueueSeq.offer(event);
        if (EventQueueSeq.size() > 100)
            log.info("EventQueueSeq size=" + EventQueueSeq.size());
        if (!bl) {
            log.error("EventQueueSeq is full,size=" + EventQueueSeq.size());
        }
    }

    //The minimum binlog pos will be sent, dump to etcd
    public static void dumpBinlogPos() {
        if (threadBinlogMap.size() == 0) {
            return;
        }
        BinlogInfo seqBinlogInfo = new BinlogInfo("", 0);
        BinlogInfo perBinlogInfo = new BinlogInfo("", 0);
        for (String s : threadBinlogMap.keySet()) {
            BinlogInfo info = threadBinlogMap.get(s);
            log.info("thread[" + s + "]:fileName=" + info.getFileName() + ",pos=" + info.getPos());
            if (s.startsWith("seq priority queue")) {
                if (info.compareBinlog(seqBinlogInfo)) {
                    seqBinlogInfo = info;
                }
            } else if (info.compareBinlog(perBinlogInfo)) {
                perBinlogInfo = info;
            }
        }

        boolean perIgnore = true, seqIgnore = true;
        for (String s : threadWaitBinlogMap.keySet()) {
            if (threadWaitBinlogMap.get(s) == null) {
                continue;
            }
            if (s.startsWith("seq priority queue")) {
                seqIgnore = false;
            } else {
                perIgnore = false;
            }
        }

        BinlogInfo successBinlogInfo;
        if (seqIgnore && perIgnore) {
            successBinlogInfo = perBinlogInfo.compareBinlog(seqBinlogInfo) ? perBinlogInfo : seqBinlogInfo;
        } else if (seqIgnore || perIgnore) {
            if (seqIgnore) {
                successBinlogInfo = perBinlogInfo;
            } else {
                successBinlogInfo = seqBinlogInfo;
            }
        } else {
            successBinlogInfo = !perBinlogInfo.compareBinlog(seqBinlogInfo) ? perBinlogInfo : seqBinlogInfo;
        }

        EtcdApi.set(RongUtil.getBinlogFileKey(), successBinlogInfo.getFileName());
        EtcdApi.set(RongUtil.getBinlogPosKey(), successBinlogInfo.getPos() + "");
        log.info("update Position:{},update file:{}", successBinlogInfo.getFileName(), successBinlogInfo.getPos());
    }

    public String getThreadName() {
        return this.threadName;
    }

    private Event poolEvent() throws InterruptedException {
        if (this.threadType == Constant.THREAD_TYPE_PER) {
            return EventQueuePer.poll(1, TimeUnit.SECONDS);
        } else {
            return EventQueueSeq.poll(1, TimeUnit.SECONDS);
        }
    }

    public void run() {
        Event event;
        String dbName = "", tblName = "", action = "", binlogFilename, binloginfo, eventType;
        ArrayList<String> result = new ArrayList<>();
        long nextBinlogPosition, binlogPosition;
        while (true) {
            try {
                if (client.isMainThreadFinish()) {
                    break;
                }
                event = poolEvent();
                if (event == null) {
                    threadWaitBinlogMap.remove(this.threadName);
                    continue;
                }
                EventHeaderV4 trackableEventHeader = event.getHeader();
                eventType = trackableEventHeader.getEventType().toString();
                binlogFilename = trackableEventHeader.getBinlogFilename();
                nextBinlogPosition = trackableEventHeader.getNextPosition();
                binlogPosition = trackableEventHeader.getPosition();
                binloginfo = binlogFilename + " " + binlogPosition;
                threadWaitBinlogMap.put(this.threadName, binlogFilename + " " + nextBinlogPosition);

                result.clear();
                String[] orgName;
                switch (eventType) {
                    case "EXT_WRITE_ROWS":
                        action = "insert";
                        WriteRowsEventData wed = event.getData();
                        orgName = EventDataUtil.getDbTableName(wed.getTableId());
                        dbName = orgName[0];
                        tblName = orgName[1];
                        result = EventDataUtil.insertQueueTojson(wed.getTableId(), EventDataUtil.getInsertEventData(wed.getIncludedColumns(), wed.getRows(), new ColumnDao().getColumnByTable(dbName, tblName)), binloginfo);
                        break;
                    case "EXT_UPDATE_ROWS":
                        action = "update";
                        UpdateRowsEventData ued = event.getData();
                        orgName = EventDataUtil.getDbTableName(ued.getTableId());
                        dbName = orgName[0];
                        tblName = orgName[1];
                        result = EventDataUtil.updateQueueToJson(ued.getTableId(), EventDataUtil.getUpdateEventData(
                                ued.getIncludedColumnsBeforeUpdate(), ued.getIncludedColumns(), new ColumnDao().getColumnByTable(dbName, tblName), ued.getRows()), binloginfo);
                        break;
                    case "EXT_DELETE_ROWS":
                        action = "delete";
                        DeleteRowsEventData ded = event.getData();
                        orgName = EventDataUtil.getDbTableName(ded.getTableId());
                        dbName = orgName[0];
                        tblName = orgName[1];
                        result = EventDataUtil.deleteQueueToJson(ded.getTableId(), EventDataUtil.getDeleteEventData(ded.getIncludedColumns(), new ColumnDao().getColumnByTable(dbName, tblName), ded.getRows()), binloginfo);
                        break;
                }

                if (result.isEmpty()) {
                    log.warn("empty result");
                    continue;
                }

                for (String message : result) {
                    QueueData tmp = new QueueData();
                    tmp.setMessage(message);
                    tmp.setRoutingKey(RongUtil.getRoutingKey(dbName, tblName, action));
                    dataList.add(tmp);
                }
                boolean sendRet = false;
                while (!sendRet) {
                    sendRet = CdcClient.notifyMessageListeners(dataList);
                    if (sendRet) {
                        log.info("[thread:" + this.threadName + "]: send data:" + dataList.size());
                        dataList.clear();
                        BinlogInfo binlogInfo = new BinlogInfo(binlogFilename, nextBinlogPosition);
                        threadBinlogMap.put(this.threadName, binlogInfo);
                    } else {
                        log.warn("[thread:{}]send message fail: position:{},file:{}", this.threadName, binlogPosition, binlogFilename);
                        Thread.sleep(100);
                    }
                }
            } catch (Exception e) {
                log.warn("get event error", e);
            }
        }
    }

}


