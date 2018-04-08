package com.rong360.main;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.*;
import com.rong360.binlogutil.EventDataUtil;
import com.rong360.binlogutil.RongUtil;
import com.rong360.database.ColumnDao;
import com.rong360.etcd.EtcdApi;
import com.rong360.model.DeleteQueueData;
import com.rong360.model.InsertQueueData;
import com.rong360.model.QueueData;
import com.rong360.model.UpdateQueueData;
import com.rong360.rabbitmq.RabbitAPI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * 
 * @author zhangtao@rong360.com
 * Event event processing thread
 *
 */
public class EventHandler extends Thread{

	private static LinkedBlockingQueue<Event> EventQueuePer = new LinkedBlockingQueue<Event>();
	private static LinkedBlockingQueue<Event> EventQueueSeq = new LinkedBlockingQueue<Event>();
	public static ConcurrentHashMap<String,ConcurrentHashMap<String,String>> threadBinlogMap = 
				new ConcurrentHashMap<String,ConcurrentHashMap<String,String>>();
	private final static Logger log = LoggerFactory.getLogger(EventHandler.class);
	private BinaryLogClient client = null;
	private String threadName = "";
	private List<QueueData> dataList = new ArrayList<QueueData>();
	private long batchTime = 0;
	public  boolean pubResult = true;
	private Integer threadType = 0;

	public static void pushPerQueue(Event event){
		boolean bl =EventQueuePer.offer(event);
		if(EventQueuePer.size()>100)
			log.info("EventQueuePer size=" + EventQueuePer.size());
		if(!bl){
			log.error("EventQueuePer is full,size=" + EventQueuePer.size());
		}
	}

	public static void pushSeqQueue(Event event){
		boolean bl =EventQueueSeq.offer(event);
		if(EventQueueSeq.size()>100)
			log.info("EventQueueSeq size=" + EventQueueSeq.size());
		if(!bl){
			log.error("EventQueueSeq is full,size=" + EventQueueSeq.size());
		}
	}


	public EventHandler(String name,BinaryLogClient client,Integer threadType) {
		this.threadName = name;
		this.client = client;
		this.threadType = threadType;
	}

	public String getThreadName(){
		return this.threadName;
	}

	private Event poolEvent() throws InterruptedException{
		if(this.threadType == 1){
			return EventQueuePer.poll(1, TimeUnit.SECONDS);
		}else{
			return EventQueueSeq.poll(1, TimeUnit.SECONDS);
		}
	}
	
	//Find the smallest file and pos saved by each thread, dump to etcd
	public static void dumpBinlogPos(){
		if(threadBinlogMap.size() ==0){
			return;
		}
		int minFile = Integer.MAX_VALUE;
		long minPos = Long.MAX_VALUE;
		String minFileName = "";
		for(String s:threadBinlogMap.keySet()){
			ConcurrentHashMap<String,String> tmp = threadBinlogMap.get(s);
			String fileName = tmp.get("filename");
			long pos = Long.valueOf(tmp.get("filepos"));
			log.info("thread[" + s + "]:fileName=" + fileName + ",pos=" + pos);
			if(fileName.startsWith("mysql-bin.")){
				int file = Integer.valueOf(fileName.substring(10));
				if(minFile > file){
					minFile = file;
					minFileName = fileName;
				}
			}
			if(pos < minPos){
				minPos = pos;
			}
		}
		EtcdApi.set(RongUtil.getBinlogFileKey(),minFileName);
		EtcdApi.set(RongUtil.getBinlogPosKey(),minPos+"");
		log.info("update Position:" + minPos + ",update file:" + minFileName);
	}

	public void run(){
		Event event = null;
		while(true ){
			try{
				if(client.isMainThreadFinish()){
					break;
				}
				event = poolEvent();
				long nowTime = System.currentTimeMillis()/1000;
				if(event == null){
					if(dataList.size() >0){
						boolean sendRet = CdcClient.notifyMessageListeners(dataList);
						if(sendRet)
						{
							log.info("[thread:" + this.threadName +"]: send data:" + dataList.size());
							dataList.clear();
							batchTime = nowTime;
						}
					}
					continue;
				}
				String eventType = event.getHeader().getEventType().toString();
				ArrayList<String> result = new ArrayList<String>();
				String dbName = "";
				String tblName = "";
				String action = "";
				if(event.getData() == null)
				{
					continue;
				}
				if(eventType.equals("EXT_WRITE_ROWS")){
					action = "insert";
					WriteRowsEventData wed = event.getData();
					String[] orgName = EventDataUtil.getDbTableName(wed.getTableId());
					dbName = orgName[0];
					tblName = orgName[1];
					ArrayList<InsertQueueData> queueData = 
							EventDataUtil.getInsertEventData(wed.getIncludedColumns(), wed.getRows(), new ColumnDao().getColumnByTable(dbName, tblName));
					result = EventDataUtil.insertQueueTojson(wed.getTableId(),queueData);

				}else if(eventType.equals("EXT_UPDATE_ROWS")){
					action = "update";
					UpdateRowsEventData ued = event.getData();
					String[] orgName = EventDataUtil.getDbTableName(ued.getTableId());
					dbName = orgName[0];
					tblName = orgName[1];
					ArrayList<UpdateQueueData> queueData = 
							EventDataUtil.getUpdateEventData(
									ued.getIncludedColumnsBeforeUpdate(),ued.getIncludedColumns(),new ColumnDao().getColumnByTable(dbName, tblName),ued.getRows());
					result = EventDataUtil.updateQueueToJson(ued.getTableId(),queueData);
				}else if(eventType.equals("EXT_DELETE_ROWS")){
					action = "delete";
					DeleteRowsEventData ded = event.getData();
					String[] orgName = EventDataUtil.getDbTableName(ded.getTableId());
					dbName = orgName[0];
					tblName = orgName[1];
					ArrayList<DeleteQueueData> queueData =
							EventDataUtil.getDeleteEventData(ded.getIncludedColumns(), new ColumnDao().getColumnByTable(dbName, tblName),ded.getRows());
					result= EventDataUtil.deleteQueueToJson(ded.getTableId(),queueData);
				}
				if(result.size()>0){
					for(String message:result){
						QueueData tmp = new QueueData();
						tmp.setMessage(message);
						tmp.setRoutingKey(RongUtil.getRoutingKey(dbName, tblName, action));
						dataList.add(tmp);
					}
				}
				if(dataList.size()>=5 || nowTime -batchTime > 5){
					if(dataList.size() >0){
						boolean sendRet = CdcClient.notifyMessageListeners(dataList);
						EventHeaderV4 trackableEventHeader = (EventHeaderV4) event.getHeader();
						long nextBinlogPosition = trackableEventHeader.getNextPosition();
						if(sendRet)
						{
							log.info("[thread:" + this.threadName +"]: send data:" + dataList.size());
							dataList.clear();
							batchTime = nowTime;
							if(dataList.size() == 0){
								if (nextBinlogPosition > 0) {
									ConcurrentHashMap<String,String>  binlogPosMap = new ConcurrentHashMap<String,String>();
									binlogPosMap.put("filename", this.client.getBinlogFilename());
									binlogPosMap.put("filepos", String.valueOf(nextBinlogPosition));
									threadBinlogMap.put(this.threadName, binlogPosMap);
									binlogPosMap = null;
								}
							}
							
						}else{
							if(nextBinlogPosition >0){
								log.info("[thread:" + this.threadName +"]send message fail: Position:" + nextBinlogPosition + ",file:" + this.client.getBinlogFilename());
							}
						}
					}	
				}
			}catch(Exception e){
				log.warn(e.getMessage());
			}
		}
		shutdownpoll();
	}
	
	private static void shutdownpoll(){
		try {
			sleep(4);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}


