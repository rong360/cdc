package com.rong360.model;

import java.util.HashMap;

/**
 * 
 * @author zhangtao@rong360.com
 * insert event data package class
 *
 */
public class InsertQueueData {
	
	private HashMap<String,Object> afterInsert = null;

	public HashMap<String, Object> getAfterInsert() {
		return afterInsert;
	}

	public void setAfterInsert(HashMap<String, Object> afterInsert) {
		this.afterInsert = afterInsert;
	}
	
	

}
