package com.rong360.model;

import java.util.HashMap;

/**
 * 
 * update event data package class
 * @author zhangtao@rong360.com
 *
 */
public class UpdateQueueData {
	
	private HashMap<String,Object>  beforeUpdate = null;
	private HashMap<String,Object> 	afterUpdate = null;
	public HashMap<String, Object> getBeforeUpdate() {
		return beforeUpdate;
	}
	public void setBeforeUpdate(HashMap<String, Object> beforeUpdate) {
		this.beforeUpdate = beforeUpdate;
	}
	public HashMap<String, Object> getAfterUpdate() {
		return afterUpdate;
	}
	public void setAfterUpdate(HashMap<String, Object> afterUpdate) {
		this.afterUpdate = afterUpdate;
	}
	
	

}
