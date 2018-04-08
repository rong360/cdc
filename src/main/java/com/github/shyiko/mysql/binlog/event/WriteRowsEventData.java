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
package com.github.shyiko.mysql.binlog.event;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.BitSet;
import java.util.List;

/**
 * @author <a href="mailto:stanley.shyiko@gmail.com">Stanley Shyiko</a>
 */
public class WriteRowsEventData implements EventData {

    /**
	 * 
	 */
	private static final long serialVersionUID = -7652763574741010018L;
	private long tableId;
    private BitSet includedColumns;
    /**
     * @see com.github.shyiko.mysql.binlog.event.deserialization.AbstractRowsEventDataDeserializer
     */
    private List<Serializable[]> rows;

    public long getTableId() {
        return tableId;
    }

    public void setTableId(long tableId) {
        this.tableId = tableId;
    }

    public BitSet getIncludedColumns() {
        return includedColumns;
    }

    public void setIncludedColumns(BitSet includedColumns) {
        this.includedColumns = includedColumns;
    }

    public List<Serializable[]> getRows() {
        return rows;
    }

    public void setRows(List<Serializable[]> rows) {
        this.rows = rows;
    }
    private String convert(Serializable[] tmp){
    	String ret ="";
    	String translateValue = "";
    	try {
			for(int i =0; i <tmp.length;i++){
				Serializable c = tmp[i];
				if(c == null){
					translateValue = null;
				}else if(c instanceof byte[]){
					translateValue = new String((byte[]) c,"UTF-8");
				}else if( c instanceof java.sql.Timestamp){
					translateValue = String.valueOf(((java.sql.Timestamp) c).getTime());
				}else if(c instanceof java.sql.Date){
					translateValue = String.valueOf(((java.sql.Date) c).getTime());
				}else{
					translateValue = String.valueOf(c);
				}
				ret += translateValue + " ";
			}
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	return ret;
    }

    
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("WriteRowsEventData:");
        sb.append("{tableId=").append(tableId);
        sb.append(", includedColumns=").append(includedColumns);
        sb.append(", rows=[");
        for (Serializable[] row : rows) {
            sb.append("\n").append(convert(row)).append("\n");
        }
        if (!rows.isEmpty()) {
            sb.replace(sb.length() - 1, sb.length(), "\n");
        }
        sb.append("]}");
        return sb.toString();
    }
    
  
}
