/*
 * Copyright 2013 Stanley Shyiko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.shyiko.mysql.binlog;

import com.github.shyiko.mysql.binlog.event.*;
import com.rong360.binlogutil.EventDataUtil;
import com.rong360.binlogutil.GlobalConfig;
import com.rong360.database.ColumnDao;
import com.rong360.main.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * @author <a href="mailto:stanley.shyiko@gmail.com">Stanley Shyiko</a>
 */
public class TraceEventListener implements BinaryLogClient.EventListener {

	public static Logger log = LoggerFactory.getLogger(TraceEventListener.class);

	@Override
	public void onEvent(Event event) {
		String eventType = event.getHeader().getEventType().toString();
		String dbName = "";
		String tblName = "";
		if(event.getData() == null)
		{
			return;
		}
		if(eventType.equals("TABLE_MAP")){
			TableMapEventData ted = event.getData();
			EventDataUtil.pushToMap(ted.getTableId(), ted.getDatabase(), ted.getTable());
		}else if(eventType.equals("ROTATE")){
			log.info("catch rotate event,clear all the table schema cache");
			new ColumnDao().clearCache();
		}else if(eventType.equals("QUERY")){
			QueryEventData ded = event.getData();
			String sql = ded.getSql().replaceAll("`", "");
			Pattern pattern = Pattern.compile("(alter)(\\s+)(table)(\\s+)(\\w+)(\\s+)(.*)",Pattern.CASE_INSENSITIVE);
			Matcher matcher = pattern.matcher(sql);
			if(matcher.find()){
				tblName = matcher.group(5);
				dbName = ded.getDatabase();
				log.info( dbName + ":" + tblName + " format maybe changed,clear cache!");
				new ColumnDao().removeCache(dbName, tblName);   			
			}
			pattern = Pattern.compile("(rename)(\\s)(table)(\\s+)(\\w+)(\\s+)(to)(\\s+)(.*)",Pattern.CASE_INSENSITIVE);
			matcher = pattern.matcher(sql);
			if(matcher.find()){
				String srcTbl = matcher.group(5);
				String toTbl = matcher.group(9);
				dbName = ded.getDatabase();
				log.info("rename " + srcTbl + " to " + toTbl);
				new ColumnDao().removeCache(dbName, srcTbl);
				new ColumnDao().removeCache(dbName, toTbl);
			}
		}else if(eventType.equals("EXT_WRITE_ROWS") || eventType.equals("EXT_UPDATE_ROWS") || eventType.equals("EXT_DELETE_ROWS")){
			eventRouter(event);
		}

	}
	private void eventRouter(Event event){
		String eventType = event.getHeader().getEventType().toString();
		String orgName[] = null;
		String dbName = "";
		String tblName = "";
		if(eventType .equals("EXT_WRITE_ROWS")){
			WriteRowsEventData wed = event.getData();
			orgName = EventDataUtil.getDbTableName(wed.getTableId());
			dbName = orgName[0];
			tblName = orgName[1];
		}else if(eventType.equals("EXT_UPDATE_ROWS")){
			UpdateRowsEventData ued = event.getData();
			orgName = EventDataUtil.getDbTableName(ued.getTableId());
			dbName = orgName[0];
			tblName = orgName[1];
		}else if(eventType.equals("EXT_DELETE_ROWS")){
			DeleteRowsEventData ded = event.getData();
			orgName = EventDataUtil.getDbTableName(ded.getTableId());
			dbName = orgName[0];
			tblName = orgName[1];
		}
		Integer ret =  GlobalConfig.filterTable(dbName, tblName);
		if(ret == 1){
			EventHandler.pushPerQueue(event);
		}else if(ret == 2){
			EventHandler.pushSeqQueue(event);
		}
	}
}
